package subscribe

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/go-apibox/websocket"
)

type SubscribeHandler struct {
	connMap      map[*websocket.Conn]*Conn // 各个连接的订阅设置
	connMapMutex sync.RWMutex

	defaultSetting *SubscribeSetting // 默认配置

	objectIdKey string // 数据包中objectid字段名，如pid，为空表示不支持object

	objectDataCache      map[string]map[string]map[string]interface{} // 各对象下各数据源的数据缓存
	objectDataCacheMutex sync.RWMutex

	dataReadyMutex sync.RWMutex // 数据是否准备就绪
	dataReadyOnce  sync.Once    // 只能unlock一次

	dataCache      map[string]map[string]interface{} // 各数据源的数据缓存
	dataCacheMutex sync.RWMutex

	// 缓存所有连接订阅的对象、字段
	allObjectIdsCache []string
	allFieldsCache    map[string][]string
	minIntervalCache  uint32

	// 事件处理函数
	objectIdsChangeHandler func(s *SubscribeHandler, oldObjectIds []string, newObjectIds []string)
	fieldsChangeHandler    func(s *SubscribeHandler, oldFields map[string][]string, newFields map[string][]string)
	intervalChangeHandler  func(s *SubscribeHandler, oldInterval uint32, newInterval uint32)

	// 更新数据锁，依赖于connMapMutex
	updateDataMutex sync.Mutex

	ShowDebug bool
}

type SubscribeSetting struct {
	Fields    map[string][]string // 订阅的字段
	Interval  uint32              // 两次数据的发送间隔
	ObjectIds []string            // 要订阅的对象ID，为nil表示订阅全部
}

func (setting *SubscribeSetting) Clone() *SubscribeSetting {
	s := new(SubscribeSetting)
	s.Fields = setting.Fields
	s.Interval = setting.Interval
	if s.Interval == 0 {
		s.Interval = uint32(1)
	}
	if setting.ObjectIds != nil {
		s.ObjectIds = make([]string, 0, len(setting.ObjectIds))
		for _, objId := range setting.ObjectIds {
			s.ObjectIds = append(s.ObjectIds, objId)
		}
	}
	return s
}

type Conn struct {
	conn             *websocket.Conn
	subscribeSetting *SubscribeSetting
}

func NewSubscribeHandler(objectIdKey string) *SubscribeHandler {
	s := new(SubscribeHandler)
	s.connMap = make(map[*websocket.Conn]*Conn)

	s.defaultSetting = &SubscribeSetting{}
	s.defaultSetting.Fields = make(map[string][]string) // 默认不订阅
	s.defaultSetting.Interval = uint32(1)               // 默认1秒
	s.defaultSetting.ObjectIds = []string{}             // 默认不订阅

	s.objectIdKey = objectIdKey

	s.objectDataCache = make(map[string]map[string]map[string]interface{})

	s.dataReadyMutex.Lock() // 立即锁定，等数据准备时才解锁

	s.dataCache = make(map[string]map[string]interface{})

	s.allObjectIdsCache = make([]string, 0)
	s.allFieldsCache = make(map[string][]string)
	s.minIntervalCache = uint32(0)

	s.updateDataMutex.Lock() // 立即锁定，等待有新连接时才解锁

	return s
}

func (s *SubscribeHandler) SetDefaultFields(fields map[string][]string) {
	s.defaultSetting.Fields = fields
}

func (s *SubscribeHandler) SetDefaultInterval(interval uint32) {
	if interval > 0 {
		s.defaultSetting.Interval = interval
	}
}

func (s *SubscribeHandler) SetDefaultObjectIds(objectIds []string) {
	s.defaultSetting.ObjectIds = objectIds
}

func (s *SubscribeHandler) ServeConn(conn *websocket.Conn) {
	c := new(Conn)
	c.conn = conn
	ignoreDefaultFields := conn.Params().Get("IgnoreDefaultFields") == "Y"
	ignoreDefaultObjectIds := conn.Params().Get("IgnoreDefaultObjectIds") == "Y"
	if ignoreDefaultFields || ignoreDefaultObjectIds {
		c.subscribeSetting = s.defaultSetting.Clone()
		if ignoreDefaultFields {
			c.subscribeSetting.Fields = make(map[string][]string) // 默认不订阅
		}
		if ignoreDefaultObjectIds {
			c.subscribeSetting.ObjectIds = []string{} // 默认不订阅
		}
	} else {
		c.subscribeSetting = s.defaultSetting.Clone()
	}

	s.connMapMutex.Lock()

	// 从无连接到有连接时，解除更新数据锁
	if len(s.connMap) == 0 {
		s.updateDataMutex.Unlock()
		if s.ShowDebug {
			fmt.Println("[subscribe] data update enabled")
		}
	}

	s.connMap[conn] = c

	s.connMapMutex.Unlock()

	s.detectFieldsChange()
	s.detectObjectIdsChange()
	s.detectIntervalChange()

	// 发送订阅数据
	if s.objectIdKey == "" {
		// 不支持objectid，则从dataCache中取数据
		go s.sendData(conn)
	} else {
		// 支持objectid，从objectDataCache中取数据
		go s.sendObjectData(conn)
	}
}

// 删除objectid
// 更新指定对象的指定数据源的数据
func (s *SubscribeHandler) DeleteObject(objectIds ...string) {
	deleteObjectIdMap := make(map[string]bool, len(objectIds))
	for _, objectId := range objectIds {
		deleteObjectIdMap[objectId] = true
	}

	// 删除各连接订阅的对象ID
	s.connMapMutex.RLock()
	deletePacket := websocket.NewPacket("delete")
	for _, c := range s.connMap {
		cObjectIds := c.subscribeSetting.ObjectIds
		var deleteObjectIds []string

		if cObjectIds == nil {
			// 为空表示订阅全部
			deleteObjectIds = objectIds
		} else {
			deleteObjectIds = []string{}
			for i, objectId := range cObjectIds {
				if _, has := deleteObjectIdMap[objectId]; has {
					c.subscribeSetting.ObjectIds = append(cObjectIds[:i], cObjectIds[i+1:]...)
					deleteObjectIds = append(deleteObjectIds, objectId)
				}
			}
		}

		if len(deleteObjectIds) > 0 {
			// 发送delete数据包
			deletePacket.SetData(map[string][]string{
				s.objectIdKey: deleteObjectIds,
			})
			// 忽略发送错误
			_ = c.conn.Send(deletePacket)
		}
	}
	s.connMapMutex.RUnlock()

	// 删除对应的缓存数据
	s.objectDataCacheMutex.Lock()
	for _, objectId := range objectIds {
		delete(s.objectDataCache, objectId)
	}
	s.objectDataCacheMutex.Unlock()
}

// 向所有连接发送错误
func (s *SubscribeHandler) SendError(errCode, errMsg string) {
	packet := websocket.NewPacket("error")
	packet.SetError(errCode, errMsg)

	s.connMapMutex.Lock()
	for conn, _ := range s.connMap {
		if err := conn.Send(packet); err != nil {
			if s.ShowDebug {
				fmt.Println("[subscribe]", err.Error())
			}
		}
		delete(s.connMap, conn)
	}
	s.connMapMutex.Unlock()
}

// 关闭订阅器，断开所有连接
func (s *SubscribeHandler) Shutdown() {
	s.connMapMutex.Lock()

	for conn, _ := range s.connMap {
		delete(s.connMap, conn)
	}

	// 从有连接到无连接时，锁定更新数据锁
	if len(s.connMap) == 0 {
		s.updateDataMutex.Lock()
		if s.ShowDebug {
			fmt.Println("[subscribe] data update disabled")
		}
	}

	s.connMapMutex.Unlock()

	s.detectFieldsChange()
	s.detectObjectIdsChange()
	s.detectIntervalChange()
}

// 不支持objectid，从dataCache中取数据
func (s *SubscribeHandler) sendData(conn *websocket.Conn) {
	connCacheData := make(map[string][]interface{})
	var lastSentTime time.Time

	// 等待数据准备就绪
	s.dataReadyMutex.RLock()
	defer s.dataReadyMutex.RUnlock()

	// 是否发过包
	hasSent := false

	for {
		// 判断是否已断开连接
		s.connMapMutex.RLock()
		c, has := s.connMap[conn]
		s.connMapMutex.RUnlock()
		if !has {
			// 连接已断开
			return
		}

		data := make(map[string][]interface{})

		s.dataCacheMutex.RLock()
		for source, fields := range c.subscribeSetting.Fields {
			sourceDataCache, has := s.dataCache[source]
			if !has {
				continue
			}

			data[source] = make([]interface{}, 0, len(fields))
			for _, field := range fields {
				if fieldData, has := sourceDataCache[field]; has {
					data[source] = append(data[source], fieldData)
				} else {
					data[source] = append(data[source], nil)
				}
			}
		}
		s.dataCacheMutex.RUnlock()

		// 按数据源比较数据是否和上次发送的一样，只发送有变化的数据源的数据
		for source, sourceData := range data {
			connCacheSourceData, has := connCacheData[source]
			if !has {
				continue
			}

			// 比较各列的值
			if len(sourceData) != len(connCacheSourceData) {
				continue
			}
			noChange := true
			for i, fieldData := range sourceData {
				// 深入对比
				if !reflect.DeepEqual(fieldData, connCacheSourceData[i]) {
					noChange = false
					break
				}
			}
			if noChange {
				// 删除没有变化的数据源
				delete(data, source)
			}
		}

		if len(data) > 0 {
			packet := websocket.NewPacket("data")
			packet.SetData(data)
			if err := conn.Send(packet); err != nil {
				// 发送数据出错则退出
				if s.ShowDebug {
					fmt.Println("[subscribe]", err.Error())
				}
				return
			}
			hasSent = true

			// 更新缓存数据
			for source, sourceData := range data {
				connCacheData[source] = sourceData
			}
		} else if !hasSent {
			// 数据为空，且没有发送过包，则发一个空包
			// 如果订阅字段为空，则不发送
			s.dataCacheMutex.RLock()
			subFieldCount := len(c.subscribeSetting.Fields)
			s.dataCacheMutex.RUnlock()

			if subFieldCount > 0 {
				packet := websocket.NewPacket("data")
				packet.SetData(map[string]interface{}{})
				if err := conn.Send(packet); err != nil {
					// 发送数据出错则退出
					if s.ShowDebug {
						fmt.Println("[subscribe]", err.Error())
					}
					return
				}
				hasSent = true
			}
		}

		// 精确计算下一次需要发送数据的时间
		nextSentTime := lastSentTime.Add(time.Second * time.Duration(c.subscribeSetting.Interval))
		nowTime := time.Now()
		if nextSentTime.After(nowTime) {
			// 精确等待
			time.Sleep(nextSentTime.Sub(nowTime))
			lastSentTime = nextSentTime
		} else {
			// 不等待，直接进入下一次发送
			lastSentTime = time.Now()
		}
	}
}

// 支持objectid，从objectDataCache中取数据
func (s *SubscribeHandler) sendObjectData(conn *websocket.Conn) {
	connObjectCacheDatas := make(map[string]map[string][]interface{})
	var lastSentTime time.Time

	// 等待数据准备就绪
	s.dataReadyMutex.RLock()
	defer s.dataReadyMutex.RUnlock()

	// 是否发过包
	hasSent := false

	for {
		// 判断是否已断开连接
		s.connMapMutex.RLock()
		c, has := s.connMap[conn]
		s.connMapMutex.RUnlock()
		if !has {
			// 连接已断开
			return
		}

		objectDatas := make(map[string]map[string][]interface{})

		s.objectDataCacheMutex.RLock()

		// 获取所有对象ID
		var objectIds []string
		if c.subscribeSetting.ObjectIds != nil {
			// 订阅多个对象
			objectIds = c.subscribeSetting.ObjectIds
		} else {
			// 订阅所有对象
			objectIds = make([]string, 0, len(s.objectDataCache))
			for objectId, _ := range s.objectDataCache {
				objectIds = append(objectIds, objectId)
			}
		}

		// 删除缓存中已经不订阅的对象
		objectIdMap := make(map[string]bool, len(objectIds))
		for _, objectId := range objectIds {
			objectIdMap[objectId] = true
		}
		for objectId, _ := range connObjectCacheDatas {
			if _, has := objectIdMap[objectId]; !has {
				delete(connObjectCacheDatas, objectId)
			}
		}

		for _, objectId := range objectIds {
			objectDataCache, has := s.objectDataCache[objectId]
			if !has {
				// 对象的数据缓存不存在，或缓存不为nil时才发送
				connObjectCacheData, has := connObjectCacheDatas[objectId]
				if !has || connObjectCacheData != nil {
					objectDatas[objectId] = nil
				}
				continue
			}

			objectData := make(map[string][]interface{})
			for source, fields := range c.subscribeSetting.Fields {
				sourceDataCache, has := objectDataCache[source]
				if !has {
					continue
				}

				objectData[source] = make([]interface{}, 0, len(fields))
				for _, field := range fields {
					if fieldData, has := sourceDataCache[field]; has {
						objectData[source] = append(objectData[source], fieldData)
					} else {
						objectData[source] = append(objectData[source], nil)
					}
				}
			}

			if len(objectData) > 0 {
				if connObjectCacheData, has := connObjectCacheDatas[objectId]; has {
					// 按数据源比较数据是否和上次发送的一样，只发送有变化的数据源的数据
					for source, sourceData := range objectData {
						connCacheSourceData, has := connObjectCacheData[source]
						if !has {
							continue
						}

						// 比较各列的值
						if len(sourceData) != len(connCacheSourceData) {
							continue
						}
						noChange := true
						for i, fieldData := range sourceData {
							// 深入对比
							if !reflect.DeepEqual(fieldData, connCacheSourceData[i]) {
								noChange = false
								break
							}
						}
						if noChange {
							// 删除没有变化的数据源
							delete(objectData, source)
						}
					}
				}
			}

			if len(objectData) > 0 {
				objectDatas[objectId] = objectData
			}
		}

		s.objectDataCacheMutex.RUnlock()

		if len(objectDatas) > 0 {
			packet := websocket.NewPacket("data")
			packet.SetData(objectDatas)
			if err := conn.Send(packet); err != nil {
				// 发送数据出错则退出
				if s.ShowDebug {
					fmt.Println("[subscribe]", err.Error())
				}
				return
			}
			hasSent = true

			// 更新缓存数据
			for objectId, objectData := range objectDatas {
				if objectData == nil {
					// 此处不能删除，否则会一直发送nil
					// delete(connObjectCacheDatas, objectId)
					connObjectCacheDatas[objectId] = nil
					continue
				}

				if v, has := connObjectCacheDatas[objectId]; !has || v == nil {
					connObjectCacheDatas[objectId] = make(map[string][]interface{})
				}
				for source, sourceData := range objectData {
					connObjectCacheDatas[objectId][source] = sourceData
				}
			}
		} else if !hasSent {
			// 数据为空，且没有发送过包，则发一个空包
			// 如果订阅字段或订阅对象为空，则不发送
			s.connMapMutex.RLock()
			cond := len(c.subscribeSetting.Fields) > 0 && (c.subscribeSetting.ObjectIds == nil || len(c.subscribeSetting.ObjectIds) > 0)
			s.connMapMutex.RUnlock()
			if cond {
				packet := websocket.NewPacket("data")
				packet.SetData(map[string]interface{}{})
				if err := conn.Send(packet); err != nil {
					// 发送数据出错则退出
					if s.ShowDebug {
						fmt.Println("[subscribe]", err.Error())
					}
					return
				}
				hasSent = true
			}
		}

		// 精确计算下一次需要发送数据的时间
		nextSentTime := lastSentTime.Add(time.Second * time.Duration(c.subscribeSetting.Interval))
		nowTime := time.Now()
		if nextSentTime.After(nowTime) {
			// 精确等待
			time.Sleep(nextSentTime.Sub(nowTime))
			lastSentTime = nextSentTime
		} else {
			// 不等待，直接进入下一次发送
			lastSentTime = time.Now()
		}
	}
}

func (s *SubscribeHandler) ServeConnClosed(conn *websocket.Conn) {
	s.connMapMutex.Lock()

	delete(s.connMap, conn)

	// 从有连接到无连接时，锁定更新数据锁
	if len(s.connMap) == 0 {
		s.updateDataMutex.Lock()
		if s.ShowDebug {
			fmt.Println("[subscribe] data update disabled")
		}
	}

	s.connMapMutex.Unlock()

	// 必须调用一次，否则如果数据未准备好，发送数据goroutine会阻塞
	s.DataReady()

	s.detectFieldsChange()
	s.detectObjectIdsChange()
	s.detectIntervalChange()
}

func (s *SubscribeHandler) ServeAction(action string, conn *websocket.Conn, packetDecoder *websocket.PacketDecoder) {
	switch action {
	case "subscribe":
		fields := make(map[string][]string)
		if err := packetDecoder.ParseData(&fields); err != nil {
			fmt.Println("[subscribe]", err.Error())
			return
		}
		s.connMapMutex.Lock()
		s.connMap[conn].subscribeSetting.Fields = fields
		s.connMapMutex.Unlock()
		s.detectFieldsChange()

	case "watch":
		objectIdsData := make(map[string][]string, 0)
		if err := packetDecoder.ParseData(&objectIdsData); err != nil {
			fmt.Println("[subscribe]", err.Error())
			return
		}
		objectIds, has := objectIdsData[s.objectIdKey]
		if !has {
			return
		}

		s.connMapMutex.Lock()
		s.connMap[conn].subscribeSetting.ObjectIds = objectIds
		s.connMapMutex.Unlock()
		s.detectObjectIdsChange()

	case "interval":
		var interval uint32
		if err := packetDecoder.ParseData(&interval); err != nil {
			fmt.Println("[subscribe]", err.Error())
			return
		}

		if interval > 0 {
			s.connMapMutex.Lock()
			s.connMap[conn].subscribeSetting.Interval = interval
			s.connMapMutex.Unlock()
			s.detectIntervalChange()
		}
	}
}

// 检测订阅字段是否变化
func (s *SubscribeHandler) detectFieldsChange() {
	if s.fieldsChangeHandler == nil {
		return
	}

	newSources := s.GetAllSources()
	newFields := make(map[string][]string)

	hasFieldsChange := false
	if len(newSources) != len(s.allFieldsCache) {
		hasFieldsChange = true
		for _, source := range newSources {
			newFields[source] = s.GetAllFields(source)
		}
	} else {
		for _, source := range newSources {
			newSourceFields := s.GetAllFields(source)
			newFields[source] = newSourceFields

			// 如果已有不同，则不用再深入比较了，直接使用新的订阅值
			if hasFieldsChange {
				continue
			}

			// 检查旧的订阅缓存中是否存在这个数据源
			if oldSourceFields, has := s.allFieldsCache[source]; !has {
				hasFieldsChange = true
				continue
			} else {
				if len(newSourceFields) != len(oldSourceFields) {
					hasFieldsChange = true
					continue
				} else {
					// 排序后进行比对
					sort.Sort(sort.StringSlice(newSourceFields))
					sort.Sort(sort.StringSlice(oldSourceFields))
					for i, field := range oldSourceFields {
						if field != newSourceFields[i] {
							hasFieldsChange = true
							break
						}
					}
					if hasFieldsChange {
						continue
					}
				}
			}
		}
	}

	if hasFieldsChange {
		oldFields := s.allFieldsCache
		s.allFieldsCache = newFields
		if s.ShowDebug {
			fmt.Println("[subscribe] fields change")
			fmt.Printf("[subscribe] old fields: %#v\n", oldFields)
			fmt.Printf("[subscribe] new fields: %#v\n", newFields)
		}

		// newFields复制一份，不要传址，否则上面的sort可能会影响外部使用者
		fields := make(map[string][]string, len(newFields))
		for source, fs := range newFields {
			t := make([]string, 0, len(fs))
			t = append(t, fs...)
			fields[source] = t
		}
		s.fieldsChangeHandler(s, oldFields, fields)
	}
}

// 检测objectid是否变化
func (s *SubscribeHandler) detectObjectIdsChange() {
	if s.objectIdsChangeHandler == nil {
		return
	}

	newObjectIds := s.GetAllObjectIds()
	isObjectIdsChange := false
	if len(s.allObjectIdsCache) != len(newObjectIds) ||
		s.allObjectIdsCache == nil && newObjectIds != nil ||
		s.allObjectIdsCache != nil && newObjectIds == nil {
		isObjectIdsChange = true
	} else {
		// 排序后进行比对
		sort.Sort(sort.StringSlice(s.allObjectIdsCache))
		sort.Sort(sort.StringSlice(newObjectIds))
		for i, objectId := range s.allObjectIdsCache {
			if objectId != newObjectIds[i] {
				isObjectIdsChange = true
				break
			}
		}
	}

	if isObjectIdsChange {
		oldObjectIds := s.allObjectIdsCache
		s.allObjectIdsCache = newObjectIds
		if s.ShowDebug {
			fmt.Println("[subscribe] object ids change")
			fmt.Printf("[subscribe] old object ids: %#v\n", oldObjectIds)
			fmt.Printf("[subscribe] new object ids: %#v\n", newObjectIds)
		}

		// 删除旧的不存在的ID的缓存数据
		newObjectIdMap := make(map[string]bool, len(newObjectIds))
		for _, id := range newObjectIds {
			newObjectIdMap[id] = true
		}

		s.objectDataCacheMutex.Lock()
		for _, id := range oldObjectIds {
			if _, has := newObjectIdMap[id]; !has {
				delete(s.objectDataCache, id)
			}
		}
		s.objectDataCacheMutex.Unlock()

		s.objectIdsChangeHandler(s, oldObjectIds, newObjectIds)
	}
}

// 检测最小刷新时间是否变化
func (s *SubscribeHandler) detectIntervalChange() {
	if s.intervalChangeHandler == nil {
		return
	}

	newInterval := s.GetMinInterval()
	if newInterval > 0 && newInterval != s.minIntervalCache {
		oldInterval := s.minIntervalCache
		s.minIntervalCache = newInterval
		if s.ShowDebug {
			fmt.Println("[subscribe] interval change")
			fmt.Println("[subscribe] old interval:", oldInterval)
			fmt.Println("[subscribe] new interval:", newInterval)
		}
		s.intervalChangeHandler(s, oldInterval, newInterval)
	}
}

// 数据准备就绪
func (s *SubscribeHandler) DataReady() {
	s.dataReadyOnce.Do(func() {
		s.dataReadyMutex.Unlock()
	})
}

// 更新指定数据源的数据
func (s *SubscribeHandler) UpdateData(source string, data map[string]interface{}) {
	s.dataCacheMutex.Lock()
	s.dataCache[source] = data
	s.dataCacheMutex.Unlock()
	s.DataReady()
}

// 清空所有数据
func (s *SubscribeHandler) CleanData() {
	s.dataCacheMutex.Lock()
	s.dataCache = make(map[string]map[string]interface{})
	s.dataCacheMutex.Unlock()
	s.DataReady()
}

// 更新指定对象的指定数据源的数据
func (s *SubscribeHandler) UpdateObjectData(objectId string, source string, data map[string]interface{}) {
	s.objectDataCacheMutex.Lock()
	if _, has := s.objectDataCache[objectId]; !has {
		s.objectDataCache[objectId] = make(map[string]map[string]interface{})
	}
	s.objectDataCache[objectId][source] = data
	s.objectDataCacheMutex.Unlock()
	s.DataReady()
}

// 清空指定对象的所有数据
func (s *SubscribeHandler) CleanObjectData(objectId string) {
	s.objectDataCacheMutex.Lock()
	s.objectDataCache[objectId] = make(map[string]map[string]interface{})
	s.objectDataCacheMutex.Unlock()
	s.DataReady()
}

// 更新所有对象ID，不存在的ID将被删除
func (s *SubscribeHandler) UpdateObjectIds(objectIds []string) {
	objectIdMap := make(map[string]bool, len(objectIds))
	for _, objectId := range objectIds {
		objectIdMap[objectId] = true
	}

	deleteObjectIds := make([]string, 0)
	s.objectDataCacheMutex.RLock()
	for objectId, _ := range s.objectDataCache {
		if _, has := objectIdMap[objectId]; !has {
			deleteObjectIds = append(deleteObjectIds, objectId)
		}
	}
	s.objectDataCacheMutex.RUnlock()

	s.DeleteObject(deleteObjectIds...)
	s.DataReady()
}

// 获取所有连接订阅的对象ID列表
func (s *SubscribeHandler) GetAllObjectIds() []string {
	objectIdMap := make(map[string]bool)

	s.connMapMutex.RLock()

	// 无连接则直接返回空，表示无订阅
	if len(s.connMap) == 0 {
		s.connMapMutex.RUnlock()
		return []string{}
	}

	for _, c := range s.connMap {
		// 只要有一个连接订阅所有对象，则直接返回nil
		if c.subscribeSetting.ObjectIds == nil {
			s.connMapMutex.RUnlock()
			return nil
		}
		for _, objectId := range c.subscribeSetting.ObjectIds {
			objectIdMap[objectId] = true
		}
	}

	s.connMapMutex.RUnlock()

	objectIds := make([]string, 0)
	for objectId, _ := range objectIdMap {
		objectIds = append(objectIds, objectId)
	}

	return objectIds
}

// 获取所有连接订阅的数据源列表
func (s *SubscribeHandler) GetAllSources() []string {
	sourceMap := make(map[string]bool)

	s.connMapMutex.RLock()

	// 无连接则直接返回空，表示无订阅
	if len(s.connMap) == 0 {
		s.connMapMutex.RUnlock()
		return []string{}
	}

	for _, c := range s.connMap {
		for source, _ := range c.subscribeSetting.Fields {
			sourceMap[source] = true
		}
	}

	s.connMapMutex.RUnlock()

	sources := make([]string, 0)
	for source, _ := range sourceMap {
		sources = append(sources, source)
	}

	return sources
}

// 获取所有连接订阅的指定数据源下的字段列表
func (s *SubscribeHandler) GetAllFields(source string) []string {
	fieldMap := make(map[string]bool)

	s.connMapMutex.RLock()

	// 无连接则直接返回空，表示无订阅
	if len(s.connMap) == 0 {
		s.connMapMutex.RUnlock()
		return []string{}
	}

	allFields := make([]string, 0)
	for _, c := range s.connMap {
		for curSource, fields := range c.subscribeSetting.Fields {
			if curSource != source {
				continue
			}
			for _, field := range fields {
				if _, has := fieldMap[field]; !has {
					fieldMap[field] = true
					allFields = append(allFields, field)
				}
			}
		}
	}
	s.connMapMutex.RUnlock()

	return allFields
}

// 获取所有连接订阅的最小数据刷新间隔
func (s *SubscribeHandler) GetMinInterval() uint32 {
	var minInterval uint32 = 0

	s.connMapMutex.RLock()
	for _, c := range s.connMap {
		if minInterval == 0 || c.subscribeSetting.Interval < minInterval {
			minInterval = c.subscribeSetting.Interval
		}
		break
	}
	s.connMapMutex.RUnlock()

	return minInterval
}

// 设置订阅数据对象ID变化时的事件处理函数
func (s *SubscribeHandler) SetObjectIdsChangeHandler(handler func(s *SubscribeHandler, oldObjectIds []string, newObjectIds []string)) {
	s.objectIdsChangeHandler = handler
}

// 设置订阅数据字段变化时的事件处理函数
func (s *SubscribeHandler) SetFieldsChangeHandler(handler func(s *SubscribeHandler, oldFields map[string][]string, newFields map[string][]string)) {
	s.fieldsChangeHandler = handler
}

// 设置订阅数据刷新频率变化时的事件处理函数
func (s *SubscribeHandler) SetIntervalChangeHandler(handler func(s *SubscribeHandler, oldInterval uint32, newInterval uint32)) {
	s.intervalChangeHandler = handler
}

// 更新数据循环，仅在有连接时才执行
func (s *SubscribeHandler) Loop(loopFunc func(s *SubscribeHandler)) {
	lastDataUpdateTime := time.Now()

	for {
		s.updateDataMutex.Lock()
		s.updateDataMutex.Unlock()

		loopFunc(s)

		interval := s.GetMinInterval()

		// 精确计算下一次需要写入数据的时间
		nextDataUpdateTime := lastDataUpdateTime.Add(time.Second * time.Duration(interval))
		if s.ShowDebug {
			fmt.Printf("[subscribe] loop wait %d seconds, util %v\n", interval, nextDataUpdateTime)
		}
		nowTime := time.Now()
		if nextDataUpdateTime.After(nowTime) {
			// 精确等待
			time.Sleep(nextDataUpdateTime.Sub(nowTime))
		} else {
			// 不等待，直接进入下一次发送
		}

		lastDataUpdateTime = time.Now()
	}
}
