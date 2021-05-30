from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE Query(ID INTEGER NOT NULL ,"
                     "purpose TEXT NOT NULL,"
                     "size INTEGER NOT NULL,"
                     'CONSTRAINT "Query_pkey" PRIMARY KEY (id),'
                     "CHECK(queryID>0),"
                     "CHECK(size >= 0));"
                     "CREATE TABLE Disk(diskID INTEGER NOT NULL ,"
                     "company TEXT NOT NULL,"
                     "speed INTEGER NOT NULL,"
                     "free_space INTEGER NOT NULL,"
                     "cost INTEGER NOT NULL,"
                     "UNIQUE(diskID),"
                     "CHECK(diskID>0),"
                     "CHECK(speed>0),"
                     "CHECK(cost>0),"
                     "CHECK(free_space>=0));"
                     "CREATE TABLE Ram(ramID INTEGER NOT NULL,"
                     "company TEXT NOT NULL,"
                     "size INTEGER NOT NULL ,"
                     "UNIQUE(ramID),"
                     "CHECK(ramID>0),"
                     "CHECK(size>0));"
                     "CREATE TABLE DiskandRam(diskID INTEGER,"
                     "ramID INTEGER,"
                     "FOREIGN KEY(diskID) REFERENCES Disk(diskID) ON DELETE CASCADE,"
                     "FOREIGN KEY(ramID) REFERENCES Ram(ramID) ON DELETE CASCADE,"
                     "PRIMARY KEY (diskID,ramID));"
                     "CREATE TABLE DiskandQuery(diskID INTEGER,"
                     "queryID INTEGER,"
                     "queryPurpose TEXT NOT NULL,"
                     "querySize INTEGER NOT NULL ,"
                     "FOREIGN KEY(diskID) REFERENCES Disk(diskID) ON DELETE CASCADE,"
                     "FOREIGN KEY(queryID) REFERENCES Query(queryID) ON DELETE CASCADE,"
                     "PRIMARY KEY (diskID,queryID));"
                     "COMMIT;)")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    pass


def dropTables():
    pass


def addQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    return Query()


def deleteQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []

if __name__ == '__main__':
    print("0. Creating all tables")
    createTables()
    print("1. Add user with ID 1 and name Roei")
    addUser(1, 'Roei')
    print("2. Add user with ID 2 and name Noa")
    addUser(2, 'Noa')
    print('3. Can reinsert the same row since no commit was done')
    addUser(2, 'Noa')
    print("4. Printing all users")
    users = getUsers(printSchema=True)  # will cause printing the users, because printSchema=true in getUsers()
    print('5. Printing user in the second row')
    print(users[1]['id'], users[1]['name'])
    print("6. Printing all IDs")
    for index in range(users.size()):
        print(users[index]['ID'])
    print("7. Delete user with ID 1")
    deleteUser(1)
    print("8. Printing all users")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("9. Delete user with ID 2, but do not commit, hence it is valid only within the connection")
    deleteUser(2, False)
    print("10. Printing all users - no change")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("11. Dropping all tables - empty database")
    dropTable()