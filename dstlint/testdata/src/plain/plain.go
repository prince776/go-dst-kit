package plain

import "time"

func f() {
	go f()
	time.Sleep(time.Second)
}
