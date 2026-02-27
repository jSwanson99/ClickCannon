package metrics

import "fmt"

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div := int64(unit)
	exp := 0
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}

	for n := bytes / unit; n >= unit && exp < len(units)-2; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), units[exp+1])
}

func formatNumber(n uint64) string {
	if n < 0 {
		return "-" + formatNumber(-n)
	}

	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result []byte
	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(digit))
	}

	return string(result)
}
