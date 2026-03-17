package cache

import (
	"bytes"
	"fmt"
	"goCache/consistenthash"
	pb "goCache/groupcachepb"
	"io"
	"log"
	"net/http"
	"sync"

	"google.golang.org/protobuf/proto"
)

const (
	defaultBasePath = "/_goCache/"
	defaultReplicas = 50
	defaultRPCPath  = "/_goCache/get"
)

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

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	Stats.IncPeerHTTPRequests()
	if r.URL.Path != defaultRPCPath {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pb.Request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, "failed to unmarshal request", http.StatusBadRequest)
		return
	}

	resp := &pb.Response{}
	group := GetGroup(req.GetGroup())
	if group == nil {
		resp.Code = http.StatusNotFound
		resp.ErrMsg = fmt.Sprintf("group %s not found", req.GetGroup())
		return
	} else {
		view, err := group.Get(req.GetKey())
		if err != nil {
			resp.Code = http.StatusInternalServerError
			resp.ErrMsg = err.Error()
		} else {
			resp.Code = http.StatusOK
			resp.Value = view.ByteSlice()
		}
	}

	out, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	w.Write(out)
}

type httpGetter struct {
	baseURL string
}

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	reqBody, err := proto.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	endpoint := h.baseURL + "get"
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

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
	if out.GetCode() != http.StatusOK {
		return fmt.Errorf("server error: %s", out.GetErrMsg())
	}
	return nil
}
