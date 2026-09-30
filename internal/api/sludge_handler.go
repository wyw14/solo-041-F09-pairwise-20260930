package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type handoverRequest struct {
	FromPump   string  `json:"from_pump"`
	ToPump     string  `json:"to_pump"`
	PumpID     string  `json:"pump_id"`
	Generation uint64  `json:"generation"`
	Flow       float64 `json:"flow"`
}

type sequenceRequest struct {
	PumpID     string  `json:"pump_id"`
	Number     uint64  `json:"number"`
	Generation uint64  `json:"generation"`
	Flow       float64 `json:"flow"`
}

func (s *Server) sludgeStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"pumps": s.plant.Pumps(), "interlocks": s.plant.Interlocks(),
		"compensations": s.plant.Compensations(),
	})
}

func (s *Server) startHandover(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var input handoverRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	handover, err := s.plant.StartHandover(lineID, input.FromPump, input.ToPump)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, handover)
}

func (s *Server) confirmHandover(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var input handoverRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	handover, err := s.plant.ConfirmHandover(lineID, input.PumpID, input.Generation, input.Flow)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, handover)
}

func (s *Server) startDrain(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := s.plant.StartDrain(lineID, chi.URLParam(r, "basinID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, operation)
}

func (s *Server) startSequence(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var input sequenceRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sequence, err := s.plant.StartSludgeSequence(lineID, input.PumpID, input.Number)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, sequence)
}

func (s *Server) confirmSequenceFlow(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sequenceID, err := uuid.Parse(chi.URLParam(r, "sequenceID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var input sequenceRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sequence, err := s.plant.ConfirmSludgeFlow(lineID, sequenceID, input.Generation, input.Flow)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, sequence)
}

func (s *Server) failSequence(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sequence, err := s.plant.FailSludgeSequence(lineID)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, sequence)
}

func (s *Server) startBackwash(w http.ResponseWriter, r *http.Request) {
	lineID, err := model.ParseLineID(chi.URLParam(r, "lineID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := s.plant.StartBackwash(lineID, chi.URLParam(r, "filterID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, operation)
}

func (s *Server) releaseInterlock(w http.ResponseWriter, r *http.Request) {
	if err := s.plant.ReleaseInterlock(chi.URLParam(r, "requestID")); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"released": true})
}

func (s *Server) runCompensations(w http.ResponseWriter, r *http.Request) {
	count, err := s.plant.RunCompensations(r.Context())
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"executed": count})
}

func (s *Server) resolveCompensationReview(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "compensationID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.plant.ResolveCompensationReview(id); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"resolved": true})
}
