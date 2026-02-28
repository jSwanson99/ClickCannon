package disk

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type dataFile struct {
	Index      int
	Path       string
	Compressed bool
	LoopIndex  int
}

func getDataFiles(folderPath string) ([]dataFile, error) {
	var files []dataFile

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".native.zst") {
			files = append(files, dataFile{
				Path:       path,
				Compressed: true,
			})
		} else if strings.HasSuffix(path, ".native") {
			files = append(files, dataFile{
				Path:       path,
				Compressed: false,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	for i := range files {
		files[i].Index = i
	}

	return files, nil
}
