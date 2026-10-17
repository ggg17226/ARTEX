package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Autumn-27/artex/db"
)

const maxTaskAssetRequestBytes = 512 << 10

func writeTaskAssetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, db.ErrTaskAssetInvalid):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, db.ErrTaskAssetTaskNotFound), errors.Is(err, db.ErrTaskAssetAssetNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}

// attachTaskAssetsRequest 是 POST /api/tasks/{id}/assets 的请求体。
// P03：asset_ids 在 wire 上是 string 数组（64 位 ID 一律 string），逐项经
// parseIDField 严格解析后才转成内部 []int64；任一项非法即 400。
type attachTaskAssetsRequest struct {
	AssetIDs      []string           `json:"asset_ids"`
	SourceSummary string             `json:"source_summary"`
	Scope         companyScopeInputs `json:"scope"`
}

// assetIDs 把 wire 的 string ID 列表解析为内部 []int64（未提供 → nil）。
func (q attachTaskAssetsRequest) assetIDs() ([]int64, error) {
	return parseIDListField(q.AssetIDs)
}

func (s *Server) attachTaskAssets(w http.ResponseWriter, r *http.Request) {
	task, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTaskAssetRequestBytes)
	var request attachTaskAssetsRequest
	if err := decode(r, &request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "请求正文过大")
		} else {
			writeErr(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	request.SourceSummary = strings.TrimSpace(request.SourceSummary)
	assetIDs, err := request.assetIDs()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	taskID, _ := parseTaskID(task.ID)
	if request.Scope != nil && len(assetIDs) > 0 {
		writeErr(w, http.StatusBadRequest, "scope 与 asset_ids 不能同时提交")
		return
	}
	if request.Scope != nil {
		mutation, err := s.m.Assets().RegisterTaskAssetScopes(taskID, request.Scope)
		if err != nil {
			writeTaskAssetError(w, err)
			return
		}
		task.Notify()
		writeJSON(w, http.StatusOK, mutation)
		return
	}
	mutation, err := s.m.Assets().AttachAssetsToTask(taskID, assetIDs, request.SourceSummary)
	if err != nil {
		writeTaskAssetError(w, err)
		return
	}
	task.Notify()
	writeJSON(w, http.StatusOK, mutation)
}

func (s *Server) detachTaskAsset(w http.ResponseWriter, r *http.Request) {
	task, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	assetID, ok := pathInt(r, "assetID")
	if !ok {
		writeErr(w, http.StatusBadRequest, "bad asset id")
		return
	}
	taskID, _ := parseTaskID(task.ID)
	detached, err := s.m.Assets().DetachAssetFromTask(taskID, assetID)
	if err != nil {
		writeTaskAssetError(w, err)
		return
	}
	if !detached {
		writeErr(w, http.StatusNotFound, "asset is not associated with this task")
		return
	}
	task.Notify()
	writeJSON(w, http.StatusOK, map[string]any{"detached": i64s(assetID)})
}

func (s *Server) taskIntentAssets(w http.ResponseWriter, r *http.Request) {
	task, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	taskID, _ := parseTaskID(task.ID)
	assets, err := s.m.Assets().IntentAssets(taskID)
	if err != nil {
		writeTaskAssetError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"assets": assets})
}
