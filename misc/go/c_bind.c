typedef struct YdbResult {} YdbResult;

typedef struct YdbUserCustomData {} YdbUserCustomData;
typedef void (*YdbResultCallback)(YdbResult *result, YdbUserCustomData *data);



void c_helper_call_ydb_result_callback(YdbResultCallback cb, YdbResult *result, YdbUserCustomData *data){
	cb(result, data);
};
