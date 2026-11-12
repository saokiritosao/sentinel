package simulator

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"sentinel/internal/event"
)

type user struct {
	name    string
	country string
	ip      string
}

type Generator struct {
	rnd   *rand.Rand
	users []user
}

var homeCountries = []string{"RU", "DE", "US", "GB", "FR", "NL", "KZ", "MY"}
var foreignCountries = []string{"BR", "NG", "VN", "AR", "ZA", "ID"}

func NewGenerator(seed uint64, nUsers int) *Generator {
	g := &Generator{rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	for i := range nUsers {
		g.users = append(g.users, user{
			name:    fmt.Sprintf("user%03d@corp.example", i+1),
			country: homeCountries[g.rnd.IntN(len(homeCountries))],
			ip:      fmt.Sprintf("198.51.100.%d", 1+i%250),
		})
	}
	return g
}

func (g *Generator) Normal(n int, now time.Time) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		u := g.users[g.rnd.IntN(len(g.users))]
		outcome := event.OutcomeSuccess
		if g.rnd.Float64() < 0.05 {
			outcome = event.OutcomeFailure
		}
		out[i] = g.login(u.name, u.ip, u.country, outcome, now)
	}
	return out
}

func (g *Generator) BruteForce(now time.Time) []event.Event {
	ip := fmt.Sprintf("203.0.113.%d", 1+g.rnd.IntN(100))
	victim := g.users[g.rnd.IntN(len(g.users))]
	attempts := 15 + g.rnd.IntN(20)
	out := make([]event.Event, attempts)
	for i := range out {
		out[i] = g.login(victim.name, ip, "NL", event.OutcomeFailure, now.Add(time.Duration(i)*150*time.Millisecond))
	}
	return out
}

func (g *Generator) Spray(now time.Time) []event.Event {
	ip := fmt.Sprintf("203.0.113.%d", 101+g.rnd.IntN(100))
	n := 8 + g.rnd.IntN(8)
	out := make([]event.Event, n)
	for i, idx := range g.rnd.Perm(len(g.users))[:n] {
		out[i] = g.login(g.users[idx].name, ip, "VN", event.OutcomeFailure, now.Add(time.Duration(i)*time.Second))
	}
	return out
}

func (g *Generator) NewCountry(now time.Time) []event.Event {
	u := g.users[g.rnd.IntN(len(g.users))]
	c := foreignCountries[g.rnd.IntN(len(foreignCountries))]
	ip := fmt.Sprintf("192.0.2.%d", 1+g.rnd.IntN(250))
	return []event.Event{g.login(u.name, ip, c, event.OutcomeSuccess, now)}
}

func (g *Generator) Baseline(now time.Time) []event.Event {
	out := make([]event.Event, len(g.users))
	for i, u := range g.users {
		out[i] = g.login(u.name, u.ip, u.country, event.OutcomeSuccess, now)
	}
	return out
}

func (g *Generator) login(name, ip, country, outcome string, ts time.Time) event.Event {
	return event.Event{
		ID:        uuid.NewString(),
		Type:      event.TypeAuthLogin,
		Outcome:   outcome,
		User:      name,
		SourceIP:  ip,
		Country:   country,
		Source:    "simulator",
		Timestamp: ts.UTC(),
	}
}
