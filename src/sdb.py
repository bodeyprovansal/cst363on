#!/usr/bin/env python3
#encoding: windows-1252
from math import ceil
from schema import *
from sdbindex import *

class Row:
    def __init__(self, schema, data):
        # create a Row object.  Data can be a raw data value from the database
        # or a list or tuple of values        
        self.schema = schema
        if type(data) == str:
            # create a Row form a raw string value of the row
            if len(data) != schema.rowsize:
                print("Error. Length of rawdata does not match row size in schema.", len(data), schema.rowsize)
                return
            self.values = []
            offset = 0
            for c in schema.cols:
                if c.coltype == Column.INTEGER:
                    v = data[offset:offset + 11]
                    self.values.append(int(v))
                    offset = offset + 11
                elif c.coltype == Column.DOUBLE:
                    v = data[offset:offset + 25]
                    self.values.append(float(v))
                    offset = offset + 25
                elif c.coltype == Column.TEXT:
                    v = data[offset:offset + c.length]
                    self.values.append(v.strip())
                    offset = offset + c.length
                else:
                    print("Error. Unknown coltype in schema.", c.coltype)
                    return  
        elif (type(data) == list or type(data) == tuple) and len(data) == len(self.schema.cols):
            # create a Row from a list of values
            self.values = data
            return
        else:
            print("Error. data is not rawdata or is not a tuple or list with number of values defined in schema", type(data), len(self.schema.cols))
            return
        
    def getRaw(self):
        raw = ''
        for i in range(len(self.schema.cols)):
            if self.schema.cols[i].coltype == Column.INTEGER:
                raw = raw + ('%11d' % self.values[i])
            elif self.schema.cols[i].coltype == Column.DOUBLE:
                raw = raw + ('%25.19g' % self.values[i])
            elif self.schema.cols[i].coltype == Column.TEXT:
                raw = raw + self.values[i] + (' ' * (self.schema.cols[i].length-len(self.values[i])))
            else:
                print("Error. Unknown coltype in schema.", self.schema.cols[i].coltype)
                return None
        return raw

    def print(self):
        # for debug 
        print(self.__repr__())

    def __repr__(self):
        r = "Row\n"
        for i in range(len(self.schema.cols)):
            r = r + self.schema.cols[i].colname + ' ' + str(self.values[i]) + '\n'
        r = r + "End Row\n"
        return r
        
class BitMap:
    def __init__(self, barray=bytearray(4096)):
        self.array = barray
        if len(self.array) != 4096:
            print("Error.  Initializing BitMap requires array of 4096 bytes!")
    
    def __getitem__(self, index):
        return self.array[index]
    
    def __setitem__(self, index, value):
        self.array[index] = value
        
    def getByteArray(self):
        return self.array
        
    def findSpace(self, start_rowid):
        for i in range(start_rowid, 4096):
            if self.array[i] == 0:
                return i
        return -1
        
    def findRow(self, start_rowid):
        for i in range(start_rowid, 4096):
            if self.array[i] == 1:
                return i
        return -1
    
    def reserve(self, rowid):
        self.array[rowid] = 2

    def unreserve(self, rowid):
        self.array[rowid] = 0
    
    def unreserveAll(self):
        for i in range(4096):
            if self.array[i] == 2:
                self.array[i] = 0
                
class SimpleDB:
    def __init__(self, schema):
        if type(schema) == str:
            self.schema = Schema(schema, [])
            self.b0 = ''        # schema, fill be filled in during __open__()
            self.b1 = BitMap()  # will be read from file in __open__()
            self.data = []      # will be read from file in __open__()
            self.indexes = []   # will be created during __createIndexes__()
            self.__open__()
            self.__createIndexes__()
        else:
            self.schema = schema
            self.b0 = ''        # schema, will be filled in during create()
            self.b1 = BitMap()  # bitmap 
            self.data = []      # data blocks, will be filled in during create()
            self.indexes = []     # will be filled in during create()
         

    def __open__(self):
        # read the database from a file
        fn = self.schema.name
        file = open(fn + ".db", "rb")
        self.b0 = bytearray(file.read(4096))
        self.schema.fromString(self.b0.decode('utf-8'))
        self.b1 = BitMap(bytearray(file.read(4096)))
        rpb = 4096 // self.schema.rowsize  # number of data blocks
        nd = ceil(4096 / rpb)
        for i in range(nd):
            self.data.append(bytearray(file.read(4096)))
        file.close()

    def __createIndexes__(self):
        for i in range(len(self.schema.cols)):
            c = self.schema.cols[i]
            if c.index == True and c.unique == True:
                index = UniqueIndex(self, c.colname, i)
                index.create()
                self.indexes.append(index)
            elif c.index == True and c.unique == False:
                index = Index(self, c.colname, i)
                index.create()
                self.indexes.append(index)
            
    def create(self):
        rpb = 4096 // self.schema.rowsize  # rows per block
        nd = ceil(4096 / rpb)              # number of data blocks to hold 4096 rows
        schemaStr = self.schema.toString()
        if len(schemaStr) > 4096:
            raise Exception("Schema too large."+str(len(schemaStr)))
 
        self.b0 = bytearray(schemaStr, 'utf-8') + bytearray(4096-len(schemaStr))
        space4096 = ' ' * 4096
        for i in range(nd):
            self.data.append(bytearray(space4096, 'utf-8'))     # create empty data blocks
        self.write()
        self.__createIndexes__()

    def write(self):
        # write the database to a file
        fn = self.schema.name
        file = open(fn + ".db", "wb")
        file.write(self.b0)
        file.write(self.b1.getByteArray())
        for d in self.data:
            file.write(d)
        file.close()

    def print(self, indexes=False):
        # for debug - print contents of database
        print('rowsize', self.schema.rowsize)
        rowcount = 0
        for rowid in range(4096):
            rrow = self.getRawRow(rowid) 
            if rrow != False:
                print('(rowid, rawrow)', '(', rowid, ',', rrow, ')')
                rowcount = rowcount + 1
        print("number of rows", rowcount)
        print("number of indexes", len(self.indexes))
        if indexes:
            for index in self.indexes:
                index.print() 


    def insertRow(self, row):
        # check unique constraints
        for index in self.indexes:
            if type(index) is UniqueIndex:
                value = row.values[index.colindex]
                i = index.search(value)
                if i >= 0 and index.entries[i].value == value:
                    # unique constraint violated
                    raise Exception("Unique index vioaltion. "+index.colname+" "+str(value))
  
        rawrow = row.getRaw()
        rowid = self.__insertRawRow__(rawrow)
        if rowid >= 0:
            for index in self.indexes:
                value = row.values[index.colindex]
                index.insert(rowid, value)
        return rowid
        
    def insertRawRowId(self, rowid, row):
        self.b1[rowid] = 1
        (block, offset) = self.getBlockOffset(rowid)
        self.data[block][offset:offset + self.schema.rowsize] = bytearray(row, 'utf-8')
        return rowid

    def __insertRawRow__(self, row):
        # find available empty row from bitmap
        rowid = self.b1.findSpace(0)
        if rowid < 0:
            raise Exception("Database is out of space.")
        self.b1[rowid] = 1
        (block, offset) = self.getBlockOffset(rowid)
        self.data[block][offset:offset + self.schema.rowsize] = bytearray(row, 'utf-8')
        return rowid

    def deleteRow(self, rowid):        
        row = self.getRow(rowid)
        if row != False:
            # update indexes
            for index in self.indexes:
                value = row.values[index.colindex]
                index.delete(rowid, value)
            # change bitmap entry for rowid to 0
            self.b1[rowid] = 0
            return True
        else:
            raise Exception("Delete failure. rowid not found "+str(rowid))

    def updateRow(self, rowid, row):
        # index update, delete existing index entries and insert new ones
        # get old values of row from databsae using rowid
        oldrow = self.getRow(rowid)
        if oldrow != False:
            # check unique contraint(s) 
            for index in self.indexes:
                if isinstance(index, UniqueIndex):
                    oldvalue = oldrow.values[index.colindex]
                    newvalue = row.values[index.colindex]
                    if oldvalue != newvalue:
                        i = index.search(newvalue)
                        if i >= 0 and index.entries[i].value == newvalue:
                            raise Exception("Unique index violation. "+index.colname+" "+str(newvalue))
            
            for index in self.indexes:
                oldvalue = oldrow.values[index.colindex]
                newvalue = row.values[index.colindex]
                if oldvalue != newvalue:
                    index.delete(rowid, oldvalue)
                    index.insert(rowid, newvalue)
            rawrow = row.getRaw()
            self.updateRawRow(rowid, rawrow)
            return True
        else:
            raise Exception("Update error. rowid not found. "+str(rowid))


    def updateRawRow(self, rowid, row):
        (block, offset) = self.getBlockOffset(rowid)
        self.data[block][offset:offset + self.schema.rowsize] = bytearray(row, 'utf=8')
       
       
    def getRow(self, rowid):
        raw = self.getRawRow(rowid)
        if raw == False:
            return False
        else:
            return Row(self.schema, raw)

    def getRawRow(self, rowid):
        if self.b1[rowid] == 1: 
            (block, offset) = self.getBlockOffset(rowid)
            return self.data[block][offset:offset + self.schema.rowsize].decode('utf-8')
        else:
            return False

    def getBlockOffset(self, rowid):
        rpb = 4096 // self.schema.rowsize  # rows per block
        block = rowid // rpb                 # block number 
        offset = (rowid % rpb) * self.schema.rowsize  
        return (block, offset)


