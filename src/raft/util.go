package raft

import "log"

// Debugging
const Debug = false

func tf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 最后一个log的下标
func (rf *Raft) getLastIndex() int {
	return len(rf.logEntries) - 1 + rf.lastIncludedIndex
}

// 获取最后的任期(快照版本
func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，否则最直接len == 0
	if len(rf.logEntries)-1 == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Term
	}
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) restoreLog(curIndex int) LogEntry {
	return rf.logEntries[curIndex-rf.lastIncludedIndex]
}

// 通过快照偏移还原真实日志任期，curIndex>=rf.lastIncludedIndex
func (rf *Raft) restoreLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logEntries[curIndex-rf.lastIncludedIndex].Term
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.restoreLogTerm(newEntryBeginIndex)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
