package api

import (
	"errors"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/wyw14/cry-97/internal/monitor"
	"github.com/wyw14/cry-97/internal/process"
)

type Server struct {
	plant   *process.Plant
	health  *monitor.Service
	web     fs.FS
	handler http.Handler
}

func NewServer(plant *process.Plant, web fs.FS) (*Server, error) {
	if plant == nil || web == nil {
		return nil, errors.New("api server requires plant and web filesystem")
	}
	server := &Server{plant: plant, web: web, health: monitor.NewService(plant, plant.Clock)}
	server.handler = server.routes()
	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) routes() http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Get("/healthz", s.healthHandler)
	router.Get("/", s.page("process.html"))
	router.Get("/process", s.page("process.html"))
	router.Get("/aeration", s.page("aeration.html"))
	router.Get("/sludge", s.page("sludge.html"))
	router.Get("/alarms", s.page("alarms.html"))
	router.Handle("/assets/*", http.StripPrefix("/assets/", http.FileServer(http.FS(s.web))))
	router.Route("/api", func(api chi.Router) {
		api.Get("/process-lines", s.processLines)
		api.Post("/process-lines/{lineID}/batches", s.startBatch)
		api.Post("/process-lines/{lineID}/advance", s.advanceBatch)
		api.Get("/process-lines/{lineID}", s.lineStatus)
		api.Get("/aeration", s.aerationStatus)
		api.Post("/aeration/{lineID}/{basinID}/samples", s.aerationSample)
		api.Get("/sludge", s.sludgeStatus)
		api.Post("/sludge/{lineID}/handover", s.startHandover)
		api.Post("/sludge/{lineID}/handover/ready", s.confirmHandover)
		api.Post("/sludge/{lineID}/drain/{basinID}", s.startDrain)
		api.Post("/sludge/{lineID}/sequences", s.startSequence)
		api.Post("/sludge/{lineID}/sequences/{sequenceID}/flow", s.confirmSequenceFlow)
		api.Post("/sludge/{lineID}/sequences/fail", s.failSequence)
		api.Post("/backwash/{lineID}/{filterID}", s.startBackwash)
		api.Post("/interlocks/{requestID}/release", s.releaseInterlock)
		api.Post("/maintenance/compensations", s.runCompensations)
		api.Post("/maintenance/compensations/{compensationID}/resolve", s.resolveCompensationReview)
		api.Post("/settling/{lineID}/cycles", s.beginSettling)
		api.Post("/settling/{lineID}/cycles/advance", s.advanceSettling)
		api.Get("/alarms", s.alarmList)
		api.Post("/alarms/{alarmID}/ack", s.acknowledgeAlarm)
		api.Post("/alarms/{alarmID}/recover", s.recoverAlarm)
		api.Post("/samples", s.receiveSample)
		api.Post("/settling/{lineID}/blanket", s.updateBlanket)
		api.Post("/dosing", NewDoseHandler(s.plant).ServeHTTP)
		api.Post("/lab/results", s.labResult)
		api.Post("/emergency/{lineID}", s.emergencyStop)
	})
	return router
}

func (s *Server) healthHandler(w http.ResponseWriter, _ *http.Request) {
	health := s.health.Observe()
	status := http.StatusOK
	if !health.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, health)
}
