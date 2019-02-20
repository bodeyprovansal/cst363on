from schema import *
from sdb import *
import traceback

class ChangeRecord():
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    BEFORE = 4
    
    def __init__(self, version_id, kind, rowid, rawdata):
        self.version_id = version_id
        self.kind = kind  # INSERT, UPDATE or DELETE change record
        self.rowid = rowid
        self.change = rawdata
         

class SimpleDBV():
# SimpleDB with versioning concurrency
    def __init__(self, schema):
        self.sdb = SimpleDB(schema) 
        self.schema = self.sdb.schema
        self.row_versionid = [0] * 4096   # a version id for each row_versionid
        self.row_history = dict()       # key=rowid, value=list of committed change records
        self.transactions = dict()      # key=tranid (versionid), value = list of change records
        self.sdb.b1.unreserveAll()
        self.next_version_id = 1;
          
    def create(self):
        self.sdb.create()
          
    def write(self):
        self.sdb.write()
          
    def print(self, indexes=False):
        # for debug - print out the contents of database
        self.sdb.print(indexes)
    
    #Part 2 ver of getRow ran successfully
    #def getRow(self, rowid, version_id):
        #trnlog = self.transactions[version_id]
        #for i in range(len(trnlog) - 1, -1, -1):
            #cr = trnlog[i]
            #if cr.rowid == rowid:
                #if cr.kind == ChangeRecord.DELETE:
                    #return False
                #else:
                    #return Row(self.sdb.schema, cr.change)
        #return self.sdb.getRow(rowid)

    def getRow(self, rowid, version_id):
        trnlog = self.transactions[version_id]
        hislog = self.row_history[rowid]
        rowver = self.row_versionid[rowid]
        if rowver == version_id:
            cur = trnlog[rowver]
            return Row(self.sdb.schema, trnlog[rowver].change)
        elif rowver < version_id:
            for i in range(len(hislog) - 1):
                hisrow = hislog[i]
                if hisrow.version_id < rowver:
                    return Row(self.sdb.schema, hisrow.change)
        else:
            return self.sdb.getRow(rowid)
 
    def insertRow(self, row, version_id):
        rowid = self.sdb.b1.findSpace(0)
        self.sdb.b1.reserve(rowid)
        cr = ChangeRecord(version_id, ChangeRecord.INSERT, rowid, row.getRaw())
        self.transactions[version_id].append(cr)
        return rowid
        #return self.sdb.insertRow(row)
          
    def deleteRow(self, rowid, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.DELETE, rowid, b'')
        self.transactions[version_id].append(cr)
        return True
        #return self.sdb.deleteRow(rowid)
          
    def updateRow(self, rowid, new_row, version_id):
        cr = ChangeRecord(version_id, ChangeRecord.UPDATE, rowid, new_row.getRaw())
        self.transactions[version_id].append(cr)
        return True
        #return self.sdb.updateRow(rowid, new_row)
          
    def startTransaction(self):
        version_id = self.getNextId() 
        self.transactions[version_id] = []     # put version_id and empty list of change records into tran dictionary
        return version_id
     
    def getNextId(self):
        version_id = self.next_version_id
        self.next_version_id = self.next_version_id + 1 
        return version_id          


    #Part 2 ver of commit() ran successfully      
    #def commit(self, version_id):
        #for cr in self.transactions[version_id]:
            #if cr.kind == ChangeRecord.DELETE:
                #self.sdb.deleteRow(cr.rowid)
            #elif cr.kind == ChangeRecord.INSERT:
                #self.sdb.insertRawRowId(cr.rowid, cr.change)
                #self.sdb.b1.__setitem__(cr.rowid, 1)
            #elif cr.kind == ChangeRecord.UPDATE:
                #self.sdb.updateRawRow(cr.rowid, cr.change)
            #else:
                #print("Invalid Change Record")
                #return False
        #return True
    
    def commit(self, version_id):
        for cr in self.transactions[version_id]:
            dbver = self.row_versionid[cr.rowid]
            if dbver < version_id:
                self.rollback(version_id)
                return False
            if cr.kind == ChangeRecord.DELETE:
                hiscr = ChangeRecord(cr.version_id, ChangeRecord.DELETE, cr.rowid, cr.change)
                self.row_history[cr.rowid].insert(0, hiscr)
                self.sdb.deleteRow(cr.rowid)
                self.row_version[cr.rowid] = version_id
            elif cr.kind == ChangeRecord.INSERT:
                hiscr = ChangeRecord(cr.version_id, ChangeRecord.INSERT, cr.rowid, cr.change)
                self.row_history[cr.rowid].insert(0, hiscr)
                self.sdb.insertRawRowId(cr.rowid, cr.change)
                self.sdb.b1.__setitem__(cr.rowid, 1)
                self.row_version[cr.rowid] = version_id
            elif cr.kind == ChangeRecord.UPDATE:
                hiscr = ChangeRecord(cr.version_id, ChangeRecord.DELETE, cr.rowid, cr.change)
                self.row_history[cr.rowid].insert(0, hiscr)
                self.sdb.updateRawRow(cr.rowid, cr.change)
                self.row_version[cr.rowid] = version_id
            else:
                return False
        return True

    def rollback(self, version_id):      
        for cr in self.transactions[version_id]:
            if cr.kind == ChangeRecord.INSERT:
                self.sdb.b1.unreserve(cr.rowid)
        del self.transactions[version_id]
        return True
        
