/* guile-dbd-odbc.c - main source file
 * Copyright (C) 2018 (james_woodcock@yahoo.co.uk, https://github.com/mjwoodcock)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses/>. */

#include <time.h>
#include <guile-dbi/guile-dbi.h>
#include <libguile.h>
#include <sql.h>
#include <sqlext.h>

void __odbc_make_g_db_handle(gdbi_db_handle_t *dbh);
void __odbc_close_g_db_handle(gdbi_db_handle_t *dbh);
void __odbc_query_g_db_handle(gdbi_db_handle_t *dbh, SQLCHAR *query_str);
SCM __odbc_getrow_g_db_handle(gdbi_db_handle_t *dbh);
SCM status_cons(SQLHANDLE handle, SQLSMALLINT error, const char *message);

#define BLOB_CHUNK_SIZE 512
typedef struct blob_chunk {
	int count;
	SQLCHAR data[BLOB_CHUNK_SIZE];
	struct blob_chunk *next;
} gdbi_odbc_blob_chunk;

typedef struct {
	SQLHENV env;
	SQLHDBC dbc;
	SQLHSTMT stmt;
} gdbi_odbc_ds_t;

SCM status_cons(SQLHANDLE handle, SQLSMALLINT type, const char *message)
{
	int code = 0;
	SQLINTEGER native;
	SQLCHAR text[256] = "";
	SQLCHAR state[7];
	SQLSMALLINT len;

	if (handle != 0) {
		code = 1;
		SQLGetDiagRec(type, handle, 1, state, &native, text, sizeof(text), &len);
	} else {
		code = type;
	}

	return scm_cons(scm_from_int(code),
		scm_string_append(scm_list_2(scm_from_locale_string(message), scm_from_locale_string((char *)text))));
}

void __odbc_make_g_db_handle(gdbi_db_handle_t *dbh)
{
	SQLCHAR *db_name;
	gdbi_odbc_ds_t *db_info;
	SQLRETURN ret;

	dbh->closed = SCM_BOOL_T;

	/* check presence of connection string */
	if (scm_equal_p(scm_string_p(dbh->constr), SCM_BOOL_F) == SCM_BOOL_T) {
		dbh->status = status_cons(0, 1, "missing connection string");
		return;
	}

	db_name = (SQLCHAR *)scm_to_locale_string(dbh->constr);
	db_info = malloc(sizeof(gdbi_odbc_ds_t));
	if (db_info == NULL) {
		dbh->status = status_cons(0, 1, "out of memory");
		return;
	}

	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &db_info->env);	
	SQLSetEnvAttr(db_info->env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
	SQLAllocHandle(SQL_HANDLE_DBC, db_info->env, &db_info->dbc);	
	ret = SQLDriverConnect(db_info->dbc, NULL, db_name, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
	if (SQL_SUCCEEDED(ret)) {
		SQLAllocHandle(SQL_HANDLE_STMT, db_info->dbc, &db_info->stmt);	
		dbh->db_info = db_info;
		dbh->status = status_cons(0, 0, "db connected");
		dbh->closed = SCM_BOOL_F;
	} else {
		dbh->status = status_cons(db_info->dbc, SQL_HANDLE_DBC, "failed to connect: ");
		free(db_info);
		dbh->db_info = NULL;
	}
}

void __odbc_close_g_db_handle(gdbi_db_handle_t *dbh)
{
	if (dbh->db_info == NULL) {
		if (!dbh->in_free) {
			dbh->status = status_cons(0, 1, "dbd info not found");
			return;
		}
	}

	if (!dbh->in_free) {
		gdbi_odbc_ds_t *db_info = dbh->db_info;

		SQLEndTran(SQL_HANDLE_DBC, db_info->dbc, SQL_COMMIT);

		SQLFreeHandle(SQL_HANDLE_STMT, db_info->stmt);
		db_info->stmt = 0;
		SQLFreeHandle(SQL_HANDLE_DBC, db_info->dbc);
		db_info->dbc = 0;
		SQLFreeHandle(SQL_HANDLE_ENV, db_info->env);
		db_info->env = 0;
		free(dbh->db_info);
		dbh->db_info = NULL;
		dbh->closed = SCM_BOOL_T;
		dbh->status = status_cons(0, 0, "dbi closed");
	}
}

void __odbc_query_g_db_handle(gdbi_db_handle_t *dbh, SQLCHAR *query_str)
{
	gdbi_odbc_ds_t *db_info = dbh->db_info;
	SQLRETURN ret;

	if (db_info == NULL) {
		dbh->status = status_cons(0, 1, "invalid dbi connection");
		return;
	}

	if (strcasecmp((const char *)query_str, "commit") == 0) {
		ret = SQLEndTran(SQL_HANDLE_DBC, db_info->dbc, SQL_COMMIT);
	} else if (strcasecmp((const char *)query_str, "rollback") == 0) {
		ret = SQLEndTran(SQL_HANDLE_DBC, db_info->dbc, SQL_ROLLBACK);
	} else {
		ret = SQLExecDirect(db_info->stmt, query_str, SQL_NTS);
	}
	if (ret == SQL_NO_DATA || SQL_SUCCEEDED(ret)) {
		dbh->status = status_cons(0, 0, "query ok");
	} else {
		dbh->status = status_cons(db_info->stmt, SQL_HANDLE_STMT, "query failed: ");
	}
}

static SCM sql_time_to_string(SQL_TIME_STRUCT *tm)
{
	char str[80];

	snprintf(str, sizeof(str), "%02d:%02d:%02d",
				tm->hour, tm->minute, tm->second);

	return scm_from_locale_string(str);
}

static SCM sql_date_to_string(SQL_DATE_STRUCT *dt)
{
	char str[80];

	snprintf(str, sizeof(str), "%04d-%02d-%02d",
				dt->year, dt->month, dt->day);

	return scm_from_locale_string(str);
}

static SCM sql_timestamp_to_string(SQL_TIMESTAMP_STRUCT *ts)
{
	char str[80];

	snprintf(str, sizeof(str), "%04d-%02d-%02d %02d:%02d:%02d",
				ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second);

	return scm_from_locale_string(str);
}

static SCM blob_chunks_to_u8vector(gdbi_odbc_blob_chunk *head, int totalbytes)
{
	SCM val;
	SCM blob_size;
	int i;

	blob_size = scm_from_int32(totalbytes);
	val = scm_make_u8vector(blob_size, scm_from_int32(0));
	if (totalbytes > 0) {
		gdbi_odbc_blob_chunk *chunk = head;
		scm_t_array_handle array_handle;
		size_t val_size;
		ssize_t val_step;
		SQLCHAR *p, *pend;

		scm_t_uint8 *elt = scm_u8vector_writable_elements(val,
							&array_handle, &val_size, &val_step);
		p = chunk->data;
		pend = p + chunk->count;
		for (i = 0; i < val_size; i++, elt += val_step) {
			*elt = *p++;
			if (p == pend) {
				chunk = head->next;
				free(head);
				head = chunk;
				p = chunk->data;
				pend = p + chunk->count;
			}
		}
		scm_array_handle_release(&array_handle);
	}

	return val;
}

SCM __odbc_getrow_g_db_handle(gdbi_db_handle_t *dbh)
{
	gdbi_odbc_ds_t *db_info = dbh->db_info;
	SQLRETURN ret;
	SQLSMALLINT columns;
	SCM res_row = SCM_EOL;
	SCM cur_val;
	int cur_col_idx = 0;
	char *p = NULL;

	if (db_info == NULL) {
		dbh->status = status_cons(0, 1, "invalid dbi connection");
		return SCM_BOOL_F;
	}

	SQLNumResultCols(db_info->stmt, &columns);
	if (SQL_SUCCEEDED(ret = SQLFetch(db_info->stmt))) {
		SQLSMALLINT i;
		for (i = 1; i <= columns; i++) {
			SQLSMALLINT datatype;
			SQLLEN indicator;
			SQLSMALLINT col_name_len;
			SQLSMALLINT decimal_digits;
			SQLULEN col_size;
			SQLCHAR col_name[1000];

;			cur_val = SCM_EOL;

			ret = SQLDescribeCol(db_info->stmt, i, col_name, sizeof(col_name), &col_name_len, &datatype, &col_size, &decimal_digits, NULL);
			switch (datatype) {
			case SQL_SMALLINT:
			case SQL_INTEGER:
			case SQL_TINYINT:
			case SQL_BIGINT:
				{
					SQLUINTEGER val;
					ret = SQLGetData(db_info->stmt, i, SQL_C_ULONG, &val, sizeof(val), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						cur_val = scm_from_long(val);
					}
				}
				break;
			case SQL_FLOAT:
			case SQL_DOUBLE:
				{
					SQLDOUBLE val;
					ret = SQLGetData(db_info->stmt, i, SQL_C_DOUBLE, &val, sizeof(val), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						cur_val = scm_from_double(val);
					}
				}
				break;
			case SQL_CHAR:
			case SQL_VARCHAR:
			case SQL_LONGVARCHAR:
				{
					SCM str_val = scm_from_locale_string("");
					char p[129];

					p[sizeof(p) - 1] = '\0';
					do {
						ret = SQLGetData(db_info->stmt, i, SQL_C_CHAR, p, sizeof(p) - 1, &indicator);
						if (indicator == SQL_NULL_DATA) {
							str_val = SCM_EOL;
							break;
						} else if (indicator > 0) {
							p[indicator] = '\0';
						}
						if (SQL_SUCCEEDED(ret)) {
							str_val = scm_string_append(scm_list_2(str_val, scm_from_locale_string(p)));
						}
					} while (ret != SQL_NO_DATA);

					cur_val = str_val;
				}
				break;
			case SQL_TYPE_DATE:
				{
					SQL_DATE_STRUCT dt;
					ret = SQLGetData(db_info->stmt, i, SQL_C_TYPE_DATE, &dt, sizeof(dt), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						cur_val = sql_date_to_string(&dt);
					}
				}
				break;
			case SQL_TYPE_TIME:
				{
					SQL_TIME_STRUCT tm;
					ret = SQLGetData(db_info->stmt, i, SQL_C_TYPE_TIME, &tm, sizeof(tm), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						cur_val = sql_time_to_string(&tm);
					}
				}
				break;
			case SQL_TYPE_TIMESTAMP:
				{
					SQL_TIMESTAMP_STRUCT dt;
					ret = SQLGetData(db_info->stmt, i, SQL_C_TYPE_TIMESTAMP, &dt, sizeof(dt), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						cur_val = sql_timestamp_to_string(&dt);
					}
				}
				break;
			case SQL_BINARY:
				{
					gdbi_odbc_blob_chunk *head = calloc(1, sizeof(gdbi_odbc_blob_chunk));
					gdbi_odbc_blob_chunk *tail = head;
					int numbytes;
					int totalbytes = 0;
					while ((ret = SQLGetData(db_info->stmt, i, SQL_C_BINARY, tail->data, sizeof(tail->data), &indicator)) != SQL_NO_DATA) {
						numbytes = (indicator > BLOB_CHUNK_SIZE || indicator == SQL_NO_TOTAL) ?
								BLOB_CHUNK_SIZE : indicator;
						if (indicator == SQL_NULL_DATA) {
							break;
						}
						tail->count = numbytes;
						totalbytes += numbytes;
						gdbi_odbc_blob_chunk *tmp = calloc(1, sizeof(gdbi_odbc_blob_chunk));
						tail->next = tmp;
						tail = tmp;
					}

					if (indicator != SQL_NULL_DATA) {
						cur_val = blob_chunks_to_u8vector(head, totalbytes);
					}
				}
				break;
			case SQL_BIT:
				{
					char data;
					ret = SQLGetData(db_info->stmt, i, SQL_C_BIT, &data, sizeof(data), &indicator);
					if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
						if (data) {
							cur_val = SCM_BOOL_T;
						} else {
							cur_val = SCM_BOOL_F;
						}
					}
				}
				break;
			default:
				{
					dbh->status = status_cons(0, 1, "unknown field datatype");
					return SCM_EOL;
				}
			}
			res_row = scm_append(scm_list_2(res_row,
				scm_list_1(scm_cons(scm_from_locale_string((char *)col_name), cur_val))));
			cur_col_idx++;
		}
		dbh->status = status_cons(0, 0, "row fetched");
		free(p);
		return res_row;
	} else {
		dbh->status = status_cons(0, 0, "no more rows");
		return SCM_BOOL_F;
	}
}
