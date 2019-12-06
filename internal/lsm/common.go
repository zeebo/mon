package lsm

import (
	"github.com/zeebo/errs"
)


type cleaner []func() error

func (c *cleaner) Add(cl func() error) { *c = append(*c, cl) }

func (c *cleaner) Close(err *error) {
	if err != nil && *err != nil {
		for i := len(*c) - 1; i >= 0; i-- {
			*err = errs.Combine(*err, (*c)[i]())
		}
	}
}
