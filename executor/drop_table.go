package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// DropTable command
func DropTable(c *Client, tableName string, reservePeriod time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	reserveSecond := new(int64)
	*reserveSecond = int64(reservePeriod.Seconds())
	_, err := c.Meta.DropApp(ctx, &admin.DropAppRequest{
		AppName: tableName,
		Options: &admin.DropAppOptions{
			SuccessIfNotExist: true,
			ReserveSeconds:    reserveSecond,
		},
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Dropped table \"%s\"\n", tableName)
	return nil
}
