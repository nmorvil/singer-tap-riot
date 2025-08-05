package tap

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KnutZuidema/golio"
	"github.com/KnutZuidema/golio/riot/account"
	"github.com/KnutZuidema/golio/riot/lol"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	requestsPerWindow = 100
	windowSeconds     = 120
	sleepDuration     = time.Duration(float64(windowSeconds)/float64(requestsPerWindow)*1000+200) * time.Millisecond
)

type RiotService struct {
	client *golio.Client
	apiKey string
}

func (r *RiotService) getMatchIdsByPlayer(player string, from time.Time, queueId int) ([]string, error) {
	acc, err := r.getAccount(player)
	if err != nil {
		return nil, err
	}
	res := r.client.Riot.LoL.Match.ListStream(acc.Puuid, &lol.MatchListOptions{
		Queue:     &queueId,
		StartTime: from,
	})
	time.Sleep(sleepDuration)
	matchIds := make([]string, 0)
	for match := range res {
		matchIds = append(matchIds, match.MatchID)
	}
	return matchIds, nil
}

func (r *RiotService) getAccount(player string) (*account.Account, error) {
	parts := strings.Split(player, "#")
	if len(parts) != 2 {
		return nil, errors.New("Invalid player id: " + player)
	}
	acc, err := r.client.Riot.Account.GetByRiotID(parts[0], parts[1])
	if err != nil {
		return nil, errors.New("Failed to get account: " + err.Error())
	}
	time.Sleep(sleepDuration)
	return acc, nil
}

func (r *RiotService) getMatchDetails(matchId string) (*lol.Match, error) {
	match, err := r.client.Riot.LoL.Match.Get(matchId)
	if err != nil {
		return nil, err
	}
	time.Sleep(sleepDuration)
	return match, nil
}

func (r *RiotService) getElo(player string) (*Elo, error) {
	acc, err := r.getAccount(player)
	if err != nil {
		return nil, err
	}
	res, err := r.client.Riot.LoL.League.ListByPuuid(acc.Puuid)
	time.Sleep(sleepDuration)
	if err != nil {
		return nil, errors.New("Failed to get league: " + err.Error())
	}
	var elo Elo
	for _, league := range res {
		if league.QueueType == "RANKED_SOLO_5x5" {
			elo = Elo{
				Puuid:        acc.Puuid,
				Date:         time.Now().Format("2006-01-02"),
				LeaguePoints: league.LeaguePoints,
				Tier:         league.Tier,
				Rank:         league.Rank,
			}
		}
	}
	return &elo, nil
}

func (r *RiotService) getMatchTimeline(matchId string) (*MatchTimeline, error) {
	url := fmt.Sprintf("https://europe.api.riotgames.com/lol/match/v5/matches/%s/timeline", matchId)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Riot-Token", r.apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	time.Sleep(sleepDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	type Participant struct {
		ParticipantID int    `json:"participantId"`
		Puuid         string `json:"puuid"`
	}

	type Frame struct {
		Timestamp         int                        `json:"timestamp"`
		ParticipantFrames map[string]json.RawMessage `json:"participantFrames"`
		Events            []lol.MatchEvent           `json:"events"`
	}

	type TimelineInfo struct {
		Participants []Participant `json:"participants"`
		Frames       []Frame       `json:"frames"`
	}

	type TimelineResponse struct {
		Info TimelineInfo `json:"info"`
	}

	var timelineResp TimelineResponse
	if err := json.Unmarshal(body, &timelineResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal timeline response: %w", err)
	}

	participantMap := make(map[int]string)
	for _, participant := range timelineResp.Info.Participants {
		participantMap[participant.ParticipantID] = participant.Puuid
	}

	var frames []MatchFrame
	var events []lol.MatchEvent

	for _, frame := range timelineResp.Info.Frames {
		timestamp := float64(frame.Timestamp)
		events = append(events, frame.Events...)
		for participantIdStr, participantFrameRaw := range frame.ParticipantFrames {
			participantId, err := strconv.Atoi(participantIdStr)
			if err != nil {
				continue
			}

			puuid, exists := participantMap[participantId]
			if !exists {
				continue
			}

			var matchFrame MatchFrame
			if err := json.Unmarshal(participantFrameRaw, &matchFrame); err != nil {
				continue
			}

			matchFrame.Timestamp = timestamp
			matchFrame.PlayerID = puuid

			frames = append(frames, matchFrame)
		}
	}

	return &MatchTimeline{frames, events, matchId}, nil
}
