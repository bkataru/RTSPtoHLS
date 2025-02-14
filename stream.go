package main

import (
	"errors"
	"log"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/format/rtspv2"
)

var (
	ErrorStreamExitNoVideoOnStream = errors.New("Stream Exit No Video On Stream")
	ErrorStreamExitRtspDisconnect  = errors.New("Stream Exit Rtsp Disconnect")
	ErrorStreamExitNoViewer        = errors.New("Stream Exit On Demand No Viewer")
)

func ServeStreams() {
	// Start the offline stream health checker
	go offlineStreamHealthChecker()

	// Start streams
	for k, v := range Config.Streams {
		go RTSPWorkerLoop(k, v.URL, v.OnDemand)
	}
}

// offlineStreamHealthChecker periodically checks offline streams
func offlineStreamHealthChecker() {
	ticker := time.NewTicker(time.Duration(Config.RetrySettings.HealthCheckIntervalMinutes) * time.Minute)
	defer ticker.Stop()

	for {
		<-ticker.C
		offlineStreams := Config.GetOfflineStreams()
		for name, stream := range offlineStreams {
			log.Printf("Checking offline stream %s\n", name)
			client, err := rtspv2.Dial(rtspv2.RTSPClientOptions{
				URL:              stream.URL,
				DisableAudio:     false,
				DialTimeout:      time.Duration(Config.RetrySettings.ConnectionTimeoutSeconds) * time.Second,
				ReadWriteTimeout: time.Duration(Config.RetrySettings.ConnectionTimeoutSeconds) * time.Second,
				Debug:            false,
			})

			if err == nil {
				log.Printf("Stream %s is back online\n", name)
				client.Close()
				Config.RemoveFromOfflineStreams(name)
				go RTSPWorkerLoop(name, stream.URL, stream.OnDemand)
			} else {
				Config.UpdateOfflineStreamLastCheck(name)
				log.Printf("Stream %s is still offline: %v\n", name, err)
			}
		}
	}
}

func RTSPWorkerLoop(name, url string, OnDemand bool) {
	defer Config.RunUnlock(name)

	retryCount := 0
	for {
		log.Println(name, "Stream Try Connect")
		err := RTSPWorker(name, url, OnDemand)

		if err != nil {
			log.Println(err)
			retryCount++

			if retryCount <= Config.RetrySettings.MaxImmediateRetries {
				// Immediate retry
				log.Printf("Immediate retry %d/%d for stream %s\n", retryCount, Config.RetrySettings.MaxImmediateRetries, name)
				time.Sleep(1 * time.Second)
				continue
			}

			if retryCount == Config.RetrySettings.MaxImmediateRetries+1 {
				// Wait for backoff period
				log.Printf("All immediate retries failed for stream %s, waiting %d minutes before final attempt\n",
					name, Config.RetrySettings.BackoffDelayMinutes)
				time.Sleep(time.Duration(Config.RetrySettings.BackoffDelayMinutes) * time.Minute)
				continue
			}

			if retryCount >= Config.RetrySettings.MaxTotalRetries {
				log.Printf("Stream %s failed after %d retries, marking as offline\n", name, retryCount)
				Config.AddToOfflineStreams(name, url, OnDemand)
				return
			}
		}

		// Reset retry count on successful connection
		retryCount = 0

		if OnDemand && !Config.HasViewer(name) {
			log.Println(name, ErrorStreamExitNoViewer)
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func RTSPWorker(name, url string, OnDemand bool) error {
	keyTest := time.NewTimer(20 * time.Second)
	clientTest := time.NewTimer(20 * time.Second)
	var preKeyTS = time.Duration(0)
	var Seq []*av.Packet
	RTSPClient, err := rtspv2.Dial(rtspv2.RTSPClientOptions{URL: url, DisableAudio: false, DialTimeout: 3 * time.Second, ReadWriteTimeout: 3 * time.Second, Debug: false})
	if err != nil {
		return err
	}
	defer RTSPClient.Close()
	if RTSPClient.CodecData != nil {
		Config.coAd(name, RTSPClient.CodecData)
	}
	var AudioOnly bool
	if len(RTSPClient.CodecData) == 1 && RTSPClient.CodecData[0].Type().IsAudio() {
		AudioOnly = true
	}
	for {
		select {
		case <-clientTest.C:
			if OnDemand && !Config.HasViewer(name) {
				return ErrorStreamExitNoViewer
			}
		case <-keyTest.C:
			return ErrorStreamExitNoVideoOnStream
		case signals := <-RTSPClient.Signals:
			switch signals {
			case rtspv2.SignalCodecUpdate:
				Config.coAd(name, RTSPClient.CodecData)
			case rtspv2.SignalStreamRTPStop:
				return ErrorStreamExitRtspDisconnect
			}
		case packetAV := <-RTSPClient.OutgoingPacketQueue:
			if AudioOnly || packetAV.IsKeyFrame {
				keyTest.Reset(20 * time.Second)
				if preKeyTS > 0 {
					Config.StreamHLSAdd(name, Seq, packetAV.Time-preKeyTS)
					Seq = []*av.Packet{}
				}
				preKeyTS = packetAV.Time
			}
			Seq = append(Seq, packetAV)
		}
	}
}
