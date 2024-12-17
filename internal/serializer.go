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
	evtDataBytes, err := Serialize(evt.GetDomainEvt())
	if err != nil {
		return nil, err
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
		DataType:      evt.GetDomainEvtName(),
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
		CreatedAt:      dbEvent.CreatedAt,
		DomainEvtName:  dbEvent.DataType,
		DomainEvtBytes: []byte(dbEvent.DataBytes),
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
	cmdDataBytes, err := Serialize(cmd.GetDomainCmd())
	if err != nil {
		return nil, err
	}
	reqCtxBytes, err := Serialize(cmd.GetReqCtx())
	if err != nil {
		return nil, err
	}
	dbCmd := &Command{
		InstanceId: cmd.GetInstanceId(),
		Uuid:       cmd.GetCommandUuid(),
		TenantUuid: cmd.GetTenantUuid(),
		Domain:     cmd.GetDomain(),
		CreatedAt:  cmd.GetCreatedAt(),
		DataType:   cmd.GetDomainCmdName(),
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
		CreatedAt:      dbCmd.CreatedAt,
		DomainCmdName:  dbCmd.DataType,
		DomainCmdBytes: []byte(dbCmd.DataBytes),
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
