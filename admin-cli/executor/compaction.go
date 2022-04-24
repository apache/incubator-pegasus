package executor

import (
	"encoding/json"
	"fmt"
)

const userSpecifiedCompaction = "user_specified_compaction"

type CompactionParams struct {
	OperationType  string
	UpdateTTLType  string
	TimeValue      uint
	HashkeyPattern string
	HashkeyMatch   string
	SortkeyPattern string
	SortkeyMatch   string
	StartTTL       int64
	StopTTL        int64
}

func SetCompaction(client *Client, tableName string, params *CompactionParams) error {
	json, err := generateCompactionEnv(client, tableName, params)
	if err != nil {
		return err
	}

	if err = SetAppEnv(client, tableName, userSpecifiedCompaction, json); err != nil {
		return err
	}
	return nil
}

// json helpers
type compactionRule struct {
	RuleType string `json:"type"`
	Params   string `json:"params"`
}
type compactionOperation struct {
	OpType string           `json:"type"`
	Params string           `json:"params"`
	Rules  []compactionRule `json:"rules"`
}
type updateTTLParams struct {
	UpdateTTLOpType string `json:"type"`
	Value           uint   `json:"value"`
}
type compactionOperations struct {
	Ops []compactionOperation `json:"ops"`
}
type keyRuleParams struct {
	Pattern   string `json:"pattern"`
	MatchType string `json:"match_type"`
}
type timeRangeRuleParams struct {
	StartTTL uint32 `json:"start_ttl"`
	StopTTL  uint32 `json:"stop_ttl"`
}

func generateCompactionEnv(client *Client, tableName string, params *CompactionParams) (string, error) {
	var err error
	var operation = &compactionOperation{}
	switch params.OperationType {
	case "delete":
		operation.OpType = "COT_DELETE"
	case "update-ttl":
		if operation, err = generateUpdateTTLOperation(params.UpdateTTLType, params.TimeValue); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("invalid operation type {%s}", params.OperationType)
	}

	if operation.Rules, err = generateRules(params); err != nil {
		return "", err
	}
	if len(operation.Rules) == 0 {
		return "", fmt.Errorf("no rules specified")
	}

	compactionJSON, err := GetAppEnv(client, tableName, userSpecifiedCompaction)
	if err != nil {
		return "", err
	}
	var operations compactionOperations
	if compactionJSON != "" {
		_ = json.Unmarshal([]byte(compactionJSON), &operations)
	}

	operations.Ops = append(operations.Ops, *operation)
	res, _ := json.Marshal(operations)
	return string(res), nil
}

var updateTTLTypeMapping = map[string]string{
	"from-now":     "UTOT_FROM_NOW",
	"from-current": "UTOT_FROM_CURRENT",
	"timestamp":    "UTOT_TIMESTAMP",
}

func generateUpdateTTLOperation(updateTTLType string, timeValue uint) (*compactionOperation, error) {
	var params updateTTLParams
	params.Value = timeValue
	ok := false
	if params.UpdateTTLOpType, ok = updateTTLTypeMapping[updateTTLType]; !ok {
		return nil, fmt.Errorf("not support the type: %s", updateTTLType)
	}

	paramsBytes, _ := json.Marshal(params)
	return &compactionOperation{
		OpType: "COT_UPDATE_TTL",
		Params: string(paramsBytes),
	}, nil
}

func generateRules(params *CompactionParams) ([]compactionRule, error) {
	var res []compactionRule
	var err error
	if params.HashkeyPattern != "" {
		var rule *compactionRule
		if rule, err = generateKeyRule("FRT_HASHKEY_PATTERN", params.HashkeyPattern, params.HashkeyMatch); err != nil {
			return nil, err
		}
		res = append(res, *rule)
	}

	if params.SortkeyPattern != "" {
		var rule *compactionRule
		if rule, err = generateKeyRule("FRT_SORTKEY_PATTERN", params.SortkeyPattern, params.SortkeyMatch); err != nil {
			return nil, err
		}
		res = append(res, *rule)
	}

	if params.StartTTL >= 0 && params.StopTTL >= 0 {
		res = append(res, generateTTLRangeRule(params.StartTTL, params.StopTTL))
	}
	return res, nil
}

var matchTypeMapping = map[string]string{
	"anywhere": "SMT_MATCH_ANYWHERE",
	"prefix":   "SMT_MATCH_PREFIX",
	"postfix":  "SMT_MATCH_POSTFIX",
}

func generateKeyRule(ruleType string, pattern string, match string) (*compactionRule, error) {
	var params keyRuleParams
	params.Pattern = pattern
	ok := false
	if params.MatchType, ok = matchTypeMapping[match]; !ok {
		return nil, fmt.Errorf("invalid match type {%s}", match)
	}

	paramsBytes, _ := json.Marshal(params)
	return &compactionRule{
		RuleType: ruleType,
		Params:   string(paramsBytes),
	}, nil
}

func generateTTLRangeRule(startTTL int64, stopTTL int64) compactionRule {
	var params timeRangeRuleParams
	params.StartTTL = uint32(startTTL)
	params.StopTTL = uint32(stopTTL)
	paramsBytes, _ := json.Marshal(params)

	return compactionRule{
		RuleType: "FRT_TTL_RANGE",
		Params:   string(paramsBytes),
	}
}
