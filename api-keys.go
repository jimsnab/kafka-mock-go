package kafkamock

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type (
	versionRange struct {
		min int16
		max int16
	}
)

const (
	ApiKeyProduce kafkaApiKey = iota // = 0
	ApiKeyFetch // = 1
	ApiKeyListOffsets // = 2
	ApiKeyMetadata // = 3
	ApiKeyLeaderAndIsr // = 4
	ApiKeyStopReplica // = 5
	ApiKeyUpdateMetadata // = 6
	ApiKeyControlledShutdown // = 7
	ApiKeyOffsetCommit // = 8
	ApiKeyOffsetFetch // = 9
	ApiKeyFindCoordinator // = 10
	ApiKeyJoinGroup // = 11
	ApiKeyHeartbeat // = 12
	ApiKeyLeaveGroup // = 13
	ApiKeySyncGroup // = 14
	ApiKeyDescribeGroups // = 15
	ApiKeyListGroups // = 16
	ApiKeySaslHandshake // = 17
	ApiKeyApiVersions // = 18
	ApiKeyCreateTopics // = 19
	ApiKeyDeleteTopics // = 20
	ApiKeyDeleteRecords // = 21
	ApiKeyInitProducerId // = 22
	ApiKeyOffsetForLeaderEpoch // = 23
	ApiKeyAddPartitionsToTxn // = 24
	ApiKeyAddOffsetsToTxn // = 25
	ApiKeyEndTxn // = 26
	ApiKeyWriteTxnMarkers // = 27
	ApiKeyTxnOffsetCommit // = 28
	ApiKeyDescribeAcls // = 29
	ApiKeyCreateAcls // = 30
	ApiKeyDeleteAcls // = 31
	ApiKeyDescribeConfigs // = 32
	ApiKeyAlterConfigs // = 33
	ApiKeyAlterReplicaLogDirs // = 34
	ApiKeyDescribeLogDirs // = 35
	ApiKeySaslAuthenticate // = 36
	ApiKeyCreatePartitions // = 37
	ApiKeyCreateDelegationToken // = 38
	ApiKeyRenewDelegationToken // = 39
	ApiKeyExpireDelegationToken // = 40
	ApiKeyDescribeDelegationToken // = 41
	ApiKeyDeleteGroups // = 42
	ApiKeyElectLeaders // = 43
	ApiKeyIncrementalAlterConfigs // = 44
	ApiKeyAlterPartitionReassignments // = 45
	ApiKeyListPartitionReassignments // = 46
	ApiKeyOffsetDelete // = 47
	ApiKeyDescribeClientQuotas // = 48
	ApiKeyAlterClientQuotas // = 49
	ApiKeyDescribeUserScramCredentials // = 50
	ApiKeyAlterUserScramCredentials // = 51
	ApiKeyUndefined52
	ApiKeyUndefined53
	ApiKeyUndefined54
	ApiKeyDescribeQuorum // = 55
	ApiKeyAlterPartition // = 56
	ApiKeyUpdateFeatures // = 57
	ApiKeyEnvelope // = 58
	ApiKeyUndefined59
	ApiKeyDescribeCluster // = 60
	ApiKeyDescribeProducers // = 61
	ApiKeyUndefined62
	ApiKeyUndefined63
	ApiKeyUnregisterBroker // = 64
	ApiKeyDescribeTransactions // = 65
	ApiKeyListTransactions // = 66
	ApiKeyAllocateProducerIds // = 67
	ApiKeyConsumerGroupHeartbeat // = 68
)

var apiVersions map[kafkaApiKey]versionRange = nil

var apiHasTags = map[string]bool{	
}

var apiNames = map[kafkaApiKey]string {
	ApiKeyProduce: "Produce",
	ApiKeyFetch: "Fetch",
	ApiKeyListOffsets: "ListOffsets",
	ApiKeyMetadata: "Metadata",
	ApiKeyLeaderAndIsr: "LeaderAndIsr",
	ApiKeyStopReplica: "StopReplica",
	ApiKeyUpdateMetadata: "UpdateMetadata",
	ApiKeyControlledShutdown: "ControlledShutdown",
	ApiKeyOffsetCommit: "OffsetCommit",
	ApiKeyOffsetFetch: "OffsetFetch",
	ApiKeyFindCoordinator: "FindCoordinator",
	ApiKeyJoinGroup: "JoinGroup",
	ApiKeyHeartbeat: "Heartbeat",
	ApiKeyLeaveGroup: "LeaveGroup",
	ApiKeySyncGroup: "SyncGroup",
	ApiKeyDescribeGroups: "DescribeGroups",
	ApiKeyListGroups: "ListGroups",
	ApiKeySaslHandshake: "SaslHandshake",
	ApiKeyApiVersions: "ApiVersions",
	ApiKeyCreateTopics: "CreateTopics",
	ApiKeyDeleteTopics: "DeleteTopics",
	ApiKeyDeleteRecords: "DeleteRecords",
	ApiKeyInitProducerId: "InitProducerId",
	ApiKeyOffsetForLeaderEpoch: "OffsetForLeaderEpoch",
	ApiKeyAddPartitionsToTxn: "AddPartitionsToTxn",
	ApiKeyAddOffsetsToTxn: "AddOffsetsToTxn",
	ApiKeyEndTxn: "EndTxn",
	ApiKeyWriteTxnMarkers: "WriteTxnMarkers",
	ApiKeyTxnOffsetCommit: "TxnOffsetCommit",
	ApiKeyDescribeAcls: "DescribeAcls",
	ApiKeyCreateAcls: "CreateAcls",
	ApiKeyDeleteAcls: "DeleteAcls",
	ApiKeyDescribeConfigs: "DescribeConfigs",
	ApiKeyAlterConfigs: "AlterConfigs",
	ApiKeyAlterReplicaLogDirs: "AlterReplicaLogDirs",
	ApiKeyDescribeLogDirs: "DescribeLogDirs",
	ApiKeySaslAuthenticate: "SaslAuthenticate",
	ApiKeyCreatePartitions: "CreatePartitions",
	ApiKeyCreateDelegationToken: "CreateDelegationToken",
	ApiKeyRenewDelegationToken: "RenewDelegationToken",
	ApiKeyExpireDelegationToken: "ExpireDelegationToken",
	ApiKeyDescribeDelegationToken: "DescribeDelegationToken",
	ApiKeyDeleteGroups: "DeleteGroups",
	ApiKeyElectLeaders: "ElectLeaders",
	ApiKeyIncrementalAlterConfigs: "IncrementalAlterConfigs",
	ApiKeyAlterPartitionReassignments: "AlterPartitionReassignments",
	ApiKeyListPartitionReassignments: "ListPartitionReassignments",
	ApiKeyOffsetDelete: "OffsetDelete",
	ApiKeyDescribeClientQuotas: "DescribeClientQuotas",
	ApiKeyAlterClientQuotas: "AlterClientQuotas",
	ApiKeyDescribeUserScramCredentials: "DescribeUserScramCredentials",
	ApiKeyAlterUserScramCredentials: "AlterUserScramCredentials",
	ApiKeyUndefined52: "Undefined52",
	ApiKeyUndefined53: "Undefined53",
	ApiKeyUndefined54: "Undefined54",
	ApiKeyDescribeQuorum: "DescribeQuorum",
	ApiKeyAlterPartition: "AlterPartition",
	ApiKeyUpdateFeatures: "UpdateFeatures",
	ApiKeyEnvelope: "Envelope",
	ApiKeyUndefined59: "Undefined59",
	ApiKeyDescribeCluster: "DescribeCluster",
	ApiKeyDescribeProducers: "DescribeProducers",
	ApiKeyUndefined62: "Undefined62",
	ApiKeyUndefined63: "Undefined63",
	ApiKeyUnregisterBroker: "UnregisterBroker",
	ApiKeyDescribeTransactions: "DescribeTransactions",
	ApiKeyListTransactions: "ListTransactions",
	ApiKeyAllocateProducerIds: "AllocateProducerIds",
	ApiKeyConsumerGroupHeartbeat: "ConsumerGroupHeartbeat",
}

var apiTable map[string]dispatchHandler
var apiTableLock sync.Mutex

func makeApiKey(apiKey kafkaApiKey, version int) string {
	return fmt.Sprintf("%d.%d", apiKey, version)
}

func initializeApis() {
	apiTableLock.Lock()
	defer apiTableLock.Unlock()

	if apiTable != nil {
		return
	}

	apiTable = map[string]dispatchHandler {
		makeApiKey(ApiKeyMetadata, 1): metadataV1,
		makeApiKey(ApiKeyFindCoordinator, 0): findCoordinatorV0,
		makeApiKey(ApiKeyOffsetFetch, 1): offsetFetchV1,
		makeApiKey(ApiKeyJoinGroup, 1): joinGroupV1,
		makeApiKey(ApiKeySyncGroup, 0): syncGroupV0,
		makeApiKey(ApiKeyLeaveGroup, 0): leaveGroupV0,
		makeApiKey(ApiKeyApiVersions, 0): apiVersionsV0,
		makeApiKey(ApiKeyHeartbeat, 0): heartbeatV0,
		makeApiKey(ApiKeyListOffsets, 1): listOffsetsV1,
		makeApiKey(ApiKeyFetch, 2): fetchV2,
	}

	apiVersions = map[kafkaApiKey]versionRange{}

	for keyVer := range apiTable {
		parts := strings.Split(keyVer, ".")
		key, _ := strconv.Atoi(parts[0])
		ver, _ := strconv.Atoi(parts[1])

		vr, exists := apiVersions[kafkaApiKey(key)]
		if !exists {
			vr = versionRange{min: int16(ver), max: int16(ver)}
			apiVersions[kafkaApiKey(key)] = vr
		} else {
			if ver < int(vr.min) {
				vr.min = int16(ver)
			}
			if ver > int(vr.max) {
				vr.max = int16(ver)
			}
		}
	}
}