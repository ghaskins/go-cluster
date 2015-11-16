package election

import (
	"fmt"
	"bytes"
)

type Vote struct {
	viewId int64
	peerId string
}

func (self *Vote) GetIndex() string {
	var buf bytes.Buffer
	buf.WriteString(self.peerId)
	buf.WriteString("-")
	buf.WriteString(fmt.Sprintf("%d", self.viewId))

	return buf.String()
}
