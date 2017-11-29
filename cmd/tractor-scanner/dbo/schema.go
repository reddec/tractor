// Code generated by go-bindata.
// sources:
// schema.sql
// DO NOT EDIT!

package dbo

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func bindataRead(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	clErr := gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}
	if clErr != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type asset struct {
	bytes []byte
	info  os.FileInfo
}

type bindataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

func (fi bindataFileInfo) Name() string {
	return fi.name
}
func (fi bindataFileInfo) Size() int64 {
	return fi.size
}
func (fi bindataFileInfo) Mode() os.FileMode {
	return fi.mode
}
func (fi bindataFileInfo) ModTime() time.Time {
	return fi.modTime
}
func (fi bindataFileInfo) IsDir() bool {
	return false
}
func (fi bindataFileInfo) Sys() interface{} {
	return nil
}

var _schemaSql = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\xd4\x94\x4f\x73\xda\x30\x10\xc5\xef\xfe\x14\x7b\x0c\x9d\x0c\xd3\x7b\x86\x83\x20\x4b\xaa\xd6\xc8\x54\x16\x0d\xf4\xa2\x71\xb1\x28\x6e\x6d\x9c\xb1\xd4\x64\xf2\xed\x3b\xf2\x5f\xd9\x30\x4e\xfa\xe7\x12\xdd\xb0\xde\xfb\xa1\x5d\xed\xd3\x82\x23\x11\x08\x82\xcc\x7d\x04\xba\x04\x16\x08\xc0\x2d\x0d\x45\x08\x87\x34\x7f\x82\x2b\x0f\x20\x89\xa1\x59\x73\x7a\x17\x22\xa7\xc4\x87\x35\xa7\x2b\xc2\x77\xf0\x09\x77\x76\xc3\xfa\xd8\xc6\xf7\xaf\x3d\x80\x7d\xa1\x22\xa3\x62\x19\x19\x10\x74\x85\xa1\x20\xab\x35\xdc\x53\xf1\xa1\xfc\x09\x5f\x03\x86\xad\x1e\x6e\x71\x49\x36\xbe\x80\xfd\xaf\xa2\x50\x27\x23\x4d\x92\x29\x6d\xa2\xec\xc1\x92\x4e\x51\xa6\xea\x7f\x16\xb8\x15\x70\x69\xb5\xa4\x0d\xa3\x9f\x37\xe8\x4d\x6e\x3c\x6f\xa4\x2a\xad\x8a\xc7\x64\xaf\xde\x68\x61\x63\x95\xa9\x47\x75\x32\x6f\xb4\xae\x91\xb2\x8e\x89\x36\x79\xf1\x3c\x2c\xec\xaf\xaa\xab\x0e\xf6\x6f\x25\x96\x6d\x96\xdd\x49\xe6\xf4\x8e\xb2\x4b\x95\xb6\x38\x8e\x4b\xe4\xc8\x16\xd8\xde\x51\x12\x4f\x3a\x54\xa6\xbf\xd7\xb4\x17\x5b\xd6\x99\xda\x23\xbd\x5c\x8e\x35\xd9\x2c\xcb\xfe\x58\xbc\xfe\xd0\xd5\x43\x50\x9f\xf9\x50\xe4\x99\xac\x43\x64\x89\x7f\x44\x6a\xc3\x57\xc3\xd2\x48\x9b\xff\x06\x2b\xd4\x83\x6a\xee\xd8\xae\xcb\x24\x17\xd6\xdc\xf3\x7b\x6b\xff\xa1\xf3\x93\x3c\xaa\x28\x56\x85\x7e\xf5\x65\x7c\xcb\xe3\x67\x77\x6f\xbe\x13\x48\xc6\x4c\xbd\x14\x07\x1c\x38\xae\x7d\xb2\x40\xf8\x42\xf1\xbe\x19\x75\xf9\x94\x98\xa3\x8c\x95\x89\x92\x54\x03\x09\x3d\x80\x10\x7d\x5c\x08\xcf\x92\x6a\xd1\xf4\xdd\x75\xf9\xb3\x1c\x87\xa9\x93\x3b\x52\x4f\x99\xb4\xdf\x5c\x8d\x93\x83\x56\xd3\x7d\xab\x94\xf6\xaa\x5d\x98\x55\x96\xb3\xd3\xc1\x4a\x89\x9b\xa9\x46\x32\x64\xe9\x43\x8f\x54\x09\xdd\xe9\xe9\x98\xfa\x30\xed\xa7\x74\x28\x3d\x63\xa7\xe7\xec\xde\x30\x39\xec\xf4\x9c\xdd\x93\x76\xbb\x1e\xc0\x92\x07\xab\xa6\xc5\x5e\x35\x45\x0c\x39\x7c\x0c\x28\xab\xc3\x1b\xb0\xf6\x0a\xda\xa7\x60\x56\xb7\x38\x89\x87\x9e\x32\x3b\x8e\xa5\x09\xe2\xac\xea\xe3\xb9\xa1\x99\x6a\x7d\xe8\xd9\x06\xa9\x9b\xd9\x96\x8d\x98\x53\xd7\x3c\x4c\xd9\xcc\xf6\x24\x89\x9d\x41\xa4\xec\x16\xb7\x97\xdf\x5d\xe9\x3c\x52\xf2\xa7\xb2\x4d\xe9\xd0\x70\xe5\xec\x4e\x6e\x7e\x07\x00\x00\xff\xff\x00\xa5\xd3\x7c\x4d\x08\x00\x00")

func schemaSqlBytes() ([]byte, error) {
	return bindataRead(
		_schemaSql,
		"schema.sql",
	)
}

func schemaSql() (*asset, error) {
	bytes, err := schemaSqlBytes()
	if err != nil {
		return nil, err
	}

	info := bindataFileInfo{name: "schema.sql", size: 2125, mode: os.FileMode(511), modTime: time.Unix(1511940273, 0)}
	a := &asset{bytes: bytes, info: info}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if err != nil {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"schema.sql": schemaSql,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		rv = append(rv, childName)
	}
	return rv, nil
}

type bintree struct {
	Func     func() (*asset, error)
	Children map[string]*bintree
}
var _bintree = &bintree{nil, map[string]*bintree{
	"schema.sql": &bintree{schemaSql, map[string]*bintree{}},
}}

// RestoreAsset restores an asset under the given directory
func RestoreAsset(dir, name string) error {
	data, err := Asset(name)
	if err != nil {
		return err
	}
	info, err := AssetInfo(name)
	if err != nil {
		return err
	}
	err = os.MkdirAll(_filePath(dir, filepath.Dir(name)), os.FileMode(0755))
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
	if err != nil {
		return err
	}
	err = os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
	if err != nil {
		return err
	}
	return nil
}

// RestoreAssets restores an asset under the given directory recursively
func RestoreAssets(dir, name string) error {
	children, err := AssetDir(name)
	// File
	if err != nil {
		return RestoreAsset(dir, name)
	}
	// Dir
	for _, child := range children {
		err = RestoreAssets(dir, filepath.Join(name, child))
		if err != nil {
			return err
		}
	}
	return nil
}

func _filePath(dir, name string) string {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	return filepath.Join(append([]string{dir}, strings.Split(cannonicalName, "/")...)...)
}

