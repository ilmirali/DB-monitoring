import cx_Oracle  # external library
# Python 3.7

DB_OBJECTS = 'DB_objects.txt'
ACCESS = 'Access.txt'
# table's/view's filed attributes list (6)
ATTRIBUTES = ['name', 'type', 'length', 'precision', 'scale', 'nullable']
source_fields_dict = dict()  # double dict: {db_object: {field: (list of attributes)}}
target_fields_dict = dict()  # double dict: {db_object: {field: (list of attributes)}}
changed_fields_dict = dict()  # dict with list of detailed changes (with values from source and target)
deleted_fields_dict = dict()  # dict with list of deleted attributes (with values from target)
new_fields_dict = dict()  # dict with list of new attributes (with values from source)


def db_connect(conn_str):
    '''
    connect to db by granted connection string
    :param conn_str: JDBC-connection string
    :return: no return
    '''
    global cursor, conn
    conn = cx_Oracle.connect(conn_str)
    cursor = conn.cursor()


def db_disconnect():
    '''
    disconnect created connect to db
    :return: no return
    '''
    global cursor
    cursor.close()


def get_db_object_desc(db_username, dbobject):
    '''
    get description of view or table
    :param db_username: schema name
    :param dbobject: view or table, in target - tables, in source - views (usually)
    :return: list of attributes (6) for each field
    '''
    try:
        query = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE " \
                "FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{}'"
        cursor.execute(query.format(dbobject))
        tmp_list = cursor.fetchall()
        return tmp_list
    except cx_Oracle.DatabaseError:
        print('cx_Oracle.DatabaseError', db_username, dbobject)


def get_fields(db_objects_list, username):
    '''
    # use get_db_object_desc function to return dict with fields by each db_object like dict (dict in dict)
    :param db_objects_list: list of views (which need to be monitored)
    :param username: db username
    :return: dict-in-dict: dict with list of views, and for each view - list of fields
    '''
    fields = dict()
    for db_object in db_objects_list:
        fields[db_object] = get_db_object_desc(username, db_object)
    return fields


def fields_with_attr_by_db_object(fields, fields_dict):
    '''
    get double dict (dict with list of all views and each view - with list of attributes)
    :param fields: one-dimensional dict (input)
    :param fields_dict: double dict (output)
    :return: fields_dict - double dict with list of attributes by each view
    '''
    tmp_dict = dict()
    for db_object in db_objects:
        for attribute in fields[db_object]:
            tmp_dict[attribute[0]] = {}  # attribute[0] - it's field name
        fields_dict[db_object] = tmp_dict
        tmp_dict = {}
        for key in fields_dict[db_object].keys():
            for attribute in fields[db_object]:
                if key == attribute[0]:
                    fields_dict[db_object][key] = attribute
    return fields_dict


def new_fields_detect(db_object):
    '''
    new fields detection
    :param db_object: view name
    :return: list of new fields with attributes from source
    '''
    tmp_list = list()
    new_fields_dict = dict()
    for key in source_fields_dict[db_object].keys():
        if key not in target_fields_dict[db_object].keys():
            field_name = key
            for key, value in source_fields_dict[db_object].items():
                if key == field_name:
                    tmp_list.append(value)
    new_fields_dict[db_object] = tmp_list.copy()
    tmp_list.clear()
    if new_fields_dict.__len__() != 0:
        return new_fields_dict[db_object]
    return {}


def deleted_fields_detect(db_object):
    '''
    deleted fields detection
    :param db_object: view name
    :return: list of deleted fields with attributes from target
    '''
    tmp_list = list()
    deleted_fields_dict = dict()
    for key in target_fields_dict[db_object].keys():
        if key not in source_fields_dict[db_object].keys():
            field_name = key
            for key, value in target_fields_dict[db_object].items():
                if key == field_name:
                    tmp_list.append(value)
    deleted_fields_dict[db_object] = tmp_list.copy()
    tmp_list.clear()
    if deleted_fields_dict.__len__() != 0:
        return deleted_fields_dict[db_object]
    return {}


def changed_fields_detect(db_object):
    '''
    changed fields detection by each of all attributes:
    type, display_size, internal_size, precision, scale, null_ok
    but if fields name change - its just new field
    :param db_object: view name
    :return: list of changed fields with attributes
    from target and source or empty list
    '''
    tmp_list = list()
    changed_fields_list = list()
    changed_fields_dict = dict()
    for key, value in target_fields_dict[db_object].items():
        if key in source_fields_dict[db_object].keys():
            target_value = value
            for value in source_fields_dict[db_object].values():
                if key == value[0]:
                    for i in range(ATTRIBUTES.__len__()):
                        if target_value[i] != value[i]:
                            if target_value[0] not in tmp_list:
                                tmp_list.append(target_value[0])
                            tmp_list.append(ATTRIBUTES[i])
                            tmp_list.append(target_username)
                            tmp_list.append(target_value[i])
                            tmp_list.append(source_username)
                            tmp_list.append(value[i])
                    if tmp_list.__len__() != 0:
                        changed_fields_list.append(tmp_list.copy())
                        tmp_list.clear()
    if changed_fields_list.__len__() != 0:
        changed_fields_dict[db_object] = changed_fields_list.copy()
        changed_fields_list.clear()
    if changed_fields_dict.__len__() != 0:
        return changed_fields_dict[db_object]
    return {}


def get_ddl_info(dbobjects):
    '''
    get created and last ddl datetime for each table/view
    from list of db objects
    :param dbobjects: list of db objects
    :return: double dict with list of db objects(views)
    and created, last ddl datetime for each db object
    '''
    dbobject = ''
    object_ddl_dict = {'CREATED': '', 'LAST_DDL_TIME': ''}
    try:
        for dbobject in dbobjects:
            query = "SELECT CREATED, LAST_DDL_TIME " \
                    "FROM USER_OBJECTS WHERE OBJECT_NAME = '{}'"
            cursor.execute(query.format(dbobject))
            tmp_list = cursor.fetchall()
            object_ddl_dict[dbobject] = {'CREATED': tmp_list[0][0], 'LAST_DDL_TIME': tmp_list[0][1]}
        return object_ddl_dict
    except:
        print('get_ddl_info function', 'Error', dbobject)


# exec dbms_stats.gather_schema_stats('datamarts'); - user_tables
def insert_row_by_db_object(dbobject):
    '''
    insert into target in VIEWS_MONITORING almost empty row
    (just with count of columns in source and target)
    for later detailed update
    :param dbobject: view name
    :return: no returned data, just update in db
    '''
    query = "INSERT INTO {}.VIEWS_MONITORING " \
            "(LOAD_DATE,TIMESTAMP,TARGET_NAME,SOURCE_NAME," \
            "DB_OBJECT,SOURCE_FIELDS_COUNT,TARGET_FIELDS_COUNT, " \
            "TARGET_CREATED, TARGET_LAST_DDL_TIME, " \
            "SOURCE_CREATED, SOURCE_LAST_DDL_TIME, " \
            "TARGET_DESC_SCRIPT, SOURCE_DESC_SCRIPT)" \
            "VALUES (TRUNC(SYSDATE), :ts, :target_schema, :source_schema, " \
            ":view_name, :count_s, :count_t," \
            ":object_target_created, :object_target_last_ddl_time," \
            ":object_source_created, :object_source_last_ddl_time," \
            ":db_object_description_t, :db_object_description_s)"
    cursor.setinputsizes(ts=cx_Oracle.TIMESTAMP)
    cursor.execute(query.format(target_username, dbobject), ts=dtime,
                   target_schema=str(target_username),
                   source_schema=str(source_username),
                   view_name=dbobject, count_s=source_fields_count,
                   count_t=target_fields_count,
                   db_object_description_t=str(target_fields[dbobject]),
                   db_object_description_s=str(source_fields[dbobject]),
                   object_target_created=target_object_ddl_dict[dbobject]['CREATED'],
                   object_target_last_ddl_time=target_object_ddl_dict[dbobject]['LAST_DDL_TIME'],
                   object_source_created=source_object_ddl_dict[dbobject]['CREATED'],
                   object_source_last_ddl_time=source_object_ddl_dict[dbobject]['LAST_DDL_TIME']
                   )
    conn.commit()


def update_row_by_db_object(dbobject, field_count_name, field_list_name,
                            field_count_value, field_list_value):
    '''
    detailed update row (view) by granted parameters
    :param dbobject: view name
    :param field_count_name: name of field to be updated (new or changed or deleted) by count of discrepancy
    :param field_list_name: name of field to be updated (new or changed or deleted) by list of discrepancy
    :param field_count_value: count of new/changed/deleted attributes
    :param field_list_value: list of new/changed/deleted attributes
    :return: no return
    '''
    query = "UPDATE {}.VIEWS_MONITORING SET {} = :fields_count, {} = :fields_list " \
            "WHERE TIMESTAMP = :ts AND DB_OBJECT = :view_name"
    cursor.setinputsizes(ts=cx_Oracle.TIMESTAMP)
    cursor.execute(query.format(target_username, field_count_name, field_list_name), ts=dtime, view_name=dbobject,
                   fields_count=field_count_value, fields_list=str(field_list_value))
    conn.commit()


def get_current_timestamp():
    '''
    get current timestamp for using it as a GUID to understand what row need to update
    for insert in table with result of compare db objects between source and target
    :return: current timestamp
    '''
    query = "SELECT SYSTIMESTAMP FROM DUAL"
    cursor.execute(query)
    datetime = cursor.fetchall()[0][0]
    return datetime


def get_list_of_db_objects(file_name):
    '''
    # get a list of views to monitor them
    :param file_name: file have the list of DB objects names (views/tables);
    may be >= 100 names of DB objects (there should be no duplicates)
    :return: list of views
    '''
    with open(file_name, "r") as txt:
        objects_of_db = [line.strip() for line in txt]
    return objects_of_db


db_objects = get_list_of_db_objects(DB_OBJECTS)

# file have source and target connection strings (2 strings for 2 different DB schemas)
with open(ACCESS, "r") as txt:
    source_conn_str = txt.readline().strip()  # JDBC connection string (source)
    target_conn_str = txt.readline().strip()  # JDBC connection string (target)

source_username = source_conn_str.split('/')[0]  # get source schema username from connection string
target_username = target_conn_str.split('/')[0]  # get target schema username from connection string

# get list of views/tables with 6 attributes for each field (from SOURCE)

db_connect(source_conn_str)
source_fields = get_fields(db_objects, source_username)
source_fields_dict = fields_with_attr_by_db_object(source_fields, source_fields_dict)
source_object_ddl_dict = get_ddl_info(db_objects)
db_disconnect()

# get list of views/tables with 6 attributes for each field (from TARGET)
db_connect(target_conn_str)
dtime = get_current_timestamp()
target_fields = get_fields(db_objects, target_username)
target_fields_dict = fields_with_attr_by_db_object(target_fields, target_fields_dict)
target_object_ddl_dict = get_ddl_info(db_objects)

# loop for detect new, deleted and changed fields and insert all discrepancy in db by each view
for db_object in db_objects:
    source_fields_count = source_fields_dict[db_object].__len__()
    target_fields_count = target_fields_dict[db_object].__len__()
    insert_row_by_db_object(db_object)

    new_fields_dict[db_object] = new_fields_detect(db_object)
    if new_fields_dict[db_object].__len__() == 0:
        del new_fields_dict[db_object]
    else:
        update_row_by_db_object(db_object,
                                'NEW_FIELDS_COUNT',
                                'NEW_FIELDS_LIST',
                                new_fields_dict[db_object].__len__(),
                                new_fields_dict[db_object])

    deleted_fields_dict[db_object] = deleted_fields_detect(db_object)
    if deleted_fields_dict[db_object].__len__() == 0:
        del deleted_fields_dict[db_object]
    else:
        update_row_by_db_object(db_object,
                                'DELETED_FIELDS_COUNT',
                                'DELETED_FIELDS_LIST',
                                deleted_fields_dict[db_object].__len__(),
                                deleted_fields_dict[db_object])

    changed_fields_dict[db_object] = changed_fields_detect(db_object)
    if changed_fields_dict[db_object].__len__() == 0:
        del changed_fields_dict[db_object]
    else:
        update_row_by_db_object(db_object,
                                'CHANGED_FIELDS_COUNT',
                                'CHANGED_FIELDS_LIST',
                                changed_fields_dict[db_object].__len__(),
                                changed_fields_dict[db_object])

db_disconnect()
