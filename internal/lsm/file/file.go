package file

import "github.com/zeebo/mon/internal/lsm/system"

type T = system.File

func Create(path string) (T, error) { return system.Create(path) }
func Open(path string) (T, error)   { return system.Open(path) }
func Rename(old, new string) error  { return system.Rename(old, new) }
func Remove(path string) error      { return system.Remove(path) }
