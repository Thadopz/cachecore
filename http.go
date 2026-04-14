package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"goCache/consistenthash"
	pb "goCache/groupcachepb"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath = "/_goCache/"
	defaultReplicas = 50
)

var defaultInvalidateTimeout = 2 * time.Second

type HTTPPool struct {
	self        string
	basePath    string
	mu          sync.RWMutex
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:        self,
		basePath:    defaultBasePath,
		httpGetters: make(map[string]*httpGetter),
	}
}

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		log.Printf("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) rpcPath(action string) string {
	return p.basePath + action
}

func (p *HTTPPool) actionForPath(path string) (string, bool) {
	switch path {
	case p.rpcPath("get"):
		return "get", true
	case p.rpcPath("invalidate"):
		return "invalidate", true
	default:
		return "", false
	}
}

func writeProtoResponse(w http.ResponseWriter, code int32, errMsg string, value []byte, notFound bool) {
	resp := &pb.Response{
		Code:     code,
		ErrMsg:   errMsg,
		Value:    value,
		NotFound: notFound,
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	Stats.IncPeerHTTPRequests()
	action, ok := p.actionForPath(r.URL.Path)
	if !ok {
		writeProtoResponse(w, http.StatusBadRequest, "bad request path", nil, false)
		return
	}
	if r.Method != http.MethodPost {
		writeProtoResponse(w, http.StatusMethodNotAllowed, "method not allowed", nil, false)
		return
	}

	var req pb.Request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProtoResponse(w, http.StatusBadRequest, "read body failed", nil, false)
		return
	}
	if err := proto.Unmarshal(body, &req); err != nil {
		writeProtoResponse(w, http.StatusBadRequest, "failed to unmarshal request", nil, false)
		return
	}

	group := GetGroup(req.GetGroup())
	if group == nil {
		writeProtoResponse(w, http.StatusNotFound, fmt.Sprintf("group %s not found", req.GetGroup()), nil, false)
		return
	}

	if action == "invalidate" {
		group.invalidateLocal(req.GetKey())
		writeProtoResponse(w, 0, "", nil, false)
		return
	}

	view, err := group.Get(r.Context(), req.GetKey())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeProtoResponse(w, http.StatusNotFound, err.Error(), nil, true)
			return
		}
		if errors.Is(err, ErrFilterNotFound) {
			writeProtoResponse(w, http.StatusNotFound, err.Error(), nil, false)
			return
		}
		writeProtoResponse(w, http.StatusInternalServerError, err.Error(), nil, false)
		return
	}
	writeProtoResponse(w, 0, "", view.ByteSlice(), false)
}

type httpGetter struct {
	baseURL string
}

func (h *httpGetter) Get(ctx context.Context, in *pb.Request, out *pb.Response) error {
	return h.doProtoRequest(ctx, "get", in, out)
}

func (h *httpGetter) Invalidate(in *pb.Request) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultInvalidateTimeout)
	defer cancel()
	return h.doProtoRequest(ctx, "invalidate", in, &pb.Response{})
}

func (h *httpGetter) doProtoRequest(ctx context.Context, action string, in *pb.Request, out *pb.Response) error {
	if ctx == nil {
		ctx = context.Background()
	}
	reqBody, err := proto.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	endpoint := h.baseURL + action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-OK status: %s", resp.Status)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}
	if err = proto.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	if out.GetNotFound() {
		return ErrNotFound
	}
	if out.GetCode() != 0 {
		return fmt.Errorf("server error code=%d: %s", out.GetCode(), out.GetErrMsg())
	}
	return nil
}

func (p *HTTPPool) BroadcastInvalidate(in *pb.Request) error {
	p.mu.RLock()
	peers := make([]*httpGetter, 0, len(p.httpGetters))
	for peer, getter := range p.httpGetters {
		if peer == p.self {
			continue
		}
		peers = append(peers, getter)
	}
	p.mu.RUnlock()

	var firstErr error
	for _, getter := range peers {
		if err := getter.Invalidate(in); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
