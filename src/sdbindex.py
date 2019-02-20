#!/usr/bin/env python3
#encoding: windows-1252
class IndexEntryU:  # unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value, rowid):
        self.value = value
        self.rowid = rowid
        
class IndexEntryNU:  # non-unique index entry
    # IndexEntry contains a value (an integer, double or string value) and a rowid 
    def __init__(self, value):
        self.value = value
        self.rowids = []
        
class Index:
    # An index consists of a list of IndexEntry objects that are in order by their value
    # To define an index specify the database object, the column name and whether the index is UNIQUE
    # after defining an index, you must call the create method to build the index entries
    def __init__(self, db, colname, colindex):
        self.db = db
        self.entries = []            # list of IndexEntry instances    
        self.colindex = colindex     # index into column list in schema
        self.colname = colname       # column name
 

    def print(self):
        # for debug - print content of index
        print("Index", self.db.schema.cols[self.colindex].colname, "Non-Unique:")
        print("number of entries", len(self.entries))
        for entry in self.entries:
            print(entry.value, entry.rowids)
        print("Index end.")

    def create(self):
        for rowid in range(4096):
            row = self.db.getRow(rowid)
            if row != False:
                value = row.values[self.colindex]
                self.insert(rowid, value)

    def delete(self, rowid, value):
        idxposition = self.search(value)
        self.entries.pop(idxposition)
        
 
    def insert(self, rowid, value):
        
        idxposition = self.search(value)
        idxentry = IndexEntryNU(value)
        if idxposition == -1:          
          idxentry.rowids.append(rowid)
          self.entries.append(idxentry)
          
        else:
          idxentry.rowids.append(rowid)
          self.entries.insert(rowid, idxentry)
          
    
    def search(self, value):
        # return index value of first IndexEntry 
        #         where IndexEntry.value >= value
        # return -1 if value is higher than all entries in index
        #Given the input value, do a binary search of the index entries. 
        #return the index position of the first index entry where 
        #index entry.value >= value. If there is no such index entry, return -1
        binfirst = 0
        binlast = len(self.entries) - 1
        idxfound = False
        
        while binfirst < binlast and not idxfound:          
          binmid = (binfirst + binlast) // 2
          entry = self.entries[binmid]
          if entry.value >= value:                     
            idxfound = True            
            return binmid
          else:
            if value < entry.value:
              binlast = binmid - 1             
            else:
              binfirst = binmid + 1
        return -1
        
            
class UniqueIndex(Index):
    # this class is code unique to index that does not allow duplicate value entries
        def __init__(self, db, colname, colindex):
            super().__init__(db, colname, colindex)
        
        def print(self):
            # for debug - print content of index s
            #print("Index", self.db.schema.cols[self.colindex].colname, "Unique:")
            print("number of entries", len(self.entries))
            for entry in self.entries:
                print(entry.value, entry.rowid)
            print("Index end.")
        
        def insert(self, rowid, value):
            idxposition = self.search(value)
            
            
            if idxposition == -1:
              idxentry = IndexEntryU(value, rowid)
              self.entries.append(idxentry)
            else:              
              print("Error: Entry is non-unique.")
    
        def delete(self, rowid, value):
          idxposition = self.search(value)
          self.entries.pop(idxposition)
        
