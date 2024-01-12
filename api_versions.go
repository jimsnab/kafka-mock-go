package kafkamock

import (
	"bufio"
	"sort"
)

type (
	apiVersionsResponseV0 struct {
		ErrorCode int16
		ApiKeys   []apiKeyV0
	}

	apiKeyV0 struct {
		ApiKey     int16
		MinVersion int16
		MaxVersion int16
	}
)

func apiVersionsV0(reader *bufio.Reader, kc *kafkaClient, clientId string, tags map[int]any) (response any, rtags map[int]any, err error) {
	keys := make([]int, 0, len(apiVersions))
	for key, vr := range apiVersions {
		if vr.min > -2 {
			keys = append(keys, int(key))
		}
	}
	sort.Ints(keys)

	apis := make([]apiKeyV0, 0, len(keys))
	for _, k := range keys {
		api := apiKeyV0{ApiKey: int16(k)}
		vr := apiVersions[kafkaApiKey(k)]
		if vr.min != -1 {
			api.MinVersion = vr.min
			api.MaxVersion = vr.max
		}
		apis = append(apis, api)
	}

	response = apiVersionsResponseV0{ApiKeys: apis}
	return
}
