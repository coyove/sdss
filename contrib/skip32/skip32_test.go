package skip32

import (
	"math/rand"
	"testing"

	"github.com/coyove/sdss/contrib/clock"
)

func TestSkip32(t *testing.T) {
	var s Skip32
	rand.Seed(clock.Unix())
	for i := 0; i < 1e6; i++ {
		x := rand.Uint32()
		res := s.Decrypt(s.Encrypt(x))
		if res != uint32(x) {
			t.Fatal(res, x)
		}
	}
}
