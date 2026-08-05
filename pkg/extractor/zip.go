package extractor

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/text/encoding/charmap"
)

type Extractor struct{}

func NewExtractor() *Extractor {
	return &Extractor{}
}

// ExtractZip extracts zip file to target dir and returns list of extracted file paths
func (e *Extractor) ExtractZip(zipPath string, targetDir string) ([]string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file %s: %w", zipPath, err)
	}
	defer r.Close()

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, err
	}

	var extractedFiles []string

	for _, f := range r.File {
		// Clean file path to prevent zip slip
		cleanName := filepath.Clean(f.Name)
		if strings.HasPrefix(cleanName, "..") {
			continue
		}

		destPath := filepath.Join(targetDir, cleanName)

		if f.FileInfo().IsDir() {
			os.MkdirAll(destPath, f.Mode())
			continue
		}

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return nil, err
		}

		outFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return nil, fmt.Errorf("failed creating extracted file %s: %w", destPath, err)
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return nil, fmt.Errorf("failed opening file inside zip %s: %w", f.Name, err)
		}

		buf := make([]byte, 1024*1024) // 1MB buffer
		_, err = io.CopyBuffer(outFile, rc, buf)
		rc.Close()
		outFile.Close()

		if err != nil {
			return nil, fmt.Errorf("failed extracting file content %s: %w", f.Name, err)
		}

		extractedFiles = append(extractedFiles, destPath)
	}

	log.Printf("[Extractor] %s descompactado com sucesso (%d arquivos).", filepath.Base(zipPath), len(extractedFiles))
	return extractedFiles, nil
}

// StreamCSVRows reads a CSV file line by line, handles ISO-8859-1 -> UTF-8 decoding, and yields rows to callback
func (e *Extractor) StreamCSVRows(filePath string, handleRow func(row []string) error) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open csv file %s: %w", filePath, err)
	}
	defer f.Close()

	// Receita Federal files are encoded in ISO-8859-1 (Latin-1)
	decoder := charmap.ISO8859_1.NewDecoder()
	reader := decoder.Reader(f)

	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow variable columns if dirty lines exist

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If CSV parse error, continue if possible
			continue
		}

		// Sanitize clean strings
		for i := range record {
			record[i] = strings.TrimSpace(record[i])
		}

		if err := handleRow(record); err != nil {
			return err
		}
	}

	return nil
}

// ConvertLatin1ToUTF8 helper
func ConvertLatin1ToUTF8(b []byte) string {
	r := charmap.ISO8859_1.NewDecoder().Reader(bytes.NewReader(b))
	out, _ := io.ReadAll(r)
	return string(out)
}
