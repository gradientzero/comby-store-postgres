package internal

import (
	"encoding/json"

	"github.com/gradientzero/comby/v2"
)

func Serialize(src interface{}) ([]byte, error) {
	if comby.IsNil(src) {
		return nil, nil
	}
	dataBytes, err := json.Marshal(src)
	if err != nil {
		return dataBytes, err
	}
	return dataBytes, nil
}

func Deserialize(dataBytes []byte, dst interface{}) error {
	return json.Unmarshal(dataBytes, dst)
}

// helpers
func BaseEventToDbEvent(evt comby.Event) (*Event, error) {
	var err error

	// serialize event data
	evtDataBytes := evt.GetDomainEvtBytes()

	// If DomainEvt is set, re-serialize to ensure consistent serialization
	if evt.GetDomainEvt() != nil {
		if evtDataBytes, err = Serialize(evt.GetDomainEvt()); err != nil {
			return nil, err
		}
	}

	// Get data type name, fallback to type name from DomainEvt if not set
	dataType := evt.GetDomainEvtName()
	if dataType == "" && evt.GetDomainEvt() != nil {
		dataType = comby.GetTypeName(evt.GetDomainEvt())
	}

	dbEvent := &Event{
		InstanceId:    evt.GetInstanceId(),
		Uuid:          evt.GetEventUuid(),
		TenantUuid:    evt.GetTenantUuid(),
		CommandUuid:   evt.GetCommandUuid(),
		Domain:        evt.GetDomain(),
		AggregateUuid: evt.GetAggregateUuid(),
		Version:       evt.GetVersion(),
		CreatedAt:     evt.GetCreatedAt(),
		DataType:      dataType,
		DataBytes:     string(evtDataBytes),
	}
	return dbEvent, nil
}

func DbEventToBaseEvent(dbEvent *Event) (comby.Event, error) {
	// Data of type base.EventData can not be deserialized
	// but within the facade the corresponding event data provider
	// will take care of this
	evt := &comby.BaseEvent{
		InstanceId:     dbEvent.InstanceId,
		EventUuid:      dbEvent.Uuid,
		TenantUuid:     dbEvent.TenantUuid,
		CommandUuid:    dbEvent.CommandUuid,
		Domain:         dbEvent.Domain,
		AggregateUuid:  dbEvent.AggregateUuid,
		Version:        dbEvent.Version,
		DomainEvtName:  dbEvent.DataType,
		DomainEvtBytes: []byte(dbEvent.DataBytes),
		DomainEvt:      nil,
		CreatedAt:      dbEvent.CreatedAt,
	}
	return evt, nil
}

func DbEventsToBaseEvents(dbEvents []*Event) ([]comby.Event, error) {
	var evts []comby.Event
	for _, dbEvent := range dbEvents {
		evt, err := DbEventToBaseEvent(dbEvent)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
	}
	return evts, nil
}

func BaseCommandToDbCommand(cmd comby.Command) (*Command, error) {
	var err error

	// serialize command data
	cmdDataBytes := cmd.GetDomainCmdBytes()

	// If DomainCmd is set, re-serialize to ensure consistent serialization
	if cmd.GetDomainCmd() != nil {
		if cmdDataBytes, err = Serialize(cmd.GetDomainCmd()); err != nil {
			return nil, err
		}
	}

	// serialize request context
	reqCtxBytes, err := Serialize(cmd.GetReqCtx())
	if err != nil {
		return nil, err
	}

	// Get data type name, fallback to type name from DomainCmd if not set
	dataType := cmd.GetDomainCmdName()
	if dataType == "" && cmd.GetDomainCmd() != nil {
		dataType = comby.GetTypeName(cmd.GetDomainCmd())
	}

	dbCmd := &Command{
		InstanceId: cmd.GetInstanceId(),
		Uuid:       cmd.GetCommandUuid(),
		TenantUuid: cmd.GetTenantUuid(),
		Domain:     cmd.GetDomain(),
		CreatedAt:  cmd.GetCreatedAt(),
		DataType:   dataType,
		DataBytes:  string(cmdDataBytes),
		ReqCtx:     string(reqCtxBytes),
	}
	return dbCmd, nil
}

func DbCommandToBaseCommand(dbCmd *Command) (comby.Command, error) {
	// Data of type base.CommandData can not be deserialized
	// but within the facade the corresponding command data provider
	// will take care of this
	var reqCtx comby.RequestContext
	if len(dbCmd.ReqCtx) > 0 {
		reqCtxBytes := []byte(dbCmd.ReqCtx)
		if err := Deserialize(reqCtxBytes, &reqCtx); err != nil {
			return nil, err
		}
	}

	cmd := &comby.BaseCommand{
		InstanceId:     dbCmd.InstanceId,
		CommandUuid:    dbCmd.Uuid,
		TenantUuid:     dbCmd.TenantUuid,
		Domain:         dbCmd.Domain,
		DomainCmdName:  dbCmd.DataType,
		DomainCmdBytes: []byte(dbCmd.DataBytes),
		DomainCmd:      nil,
		CreatedAt:      dbCmd.CreatedAt,
		ReqCtx:         &reqCtx,
	}
	return cmd, nil
}

func DbCommandsToBaseCommands(dbCommands []*Command) ([]comby.Command, error) {
	var cmds []comby.Command
	for _, dbCommand := range dbCommands {
		cmd, err := DbCommandToBaseCommand(dbCommand)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}
