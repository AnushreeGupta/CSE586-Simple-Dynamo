package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import static android.R.attr.key;

/**
 * Created by anushree on 5/1/17.
 */

public class SQLHelper extends SQLiteOpenHelper {

    /* Inner class that defines the table contents */
    public static final String DATABASE_NAME = "SimpleDHT";
    public static final String TABLE_NAME = "DataItems";
    public static final String TEST_TABLE = "TestTable";
    public static final String COLUMN_1 = "key";
    public static final String COLUMN_2 = "value";
    public static final String COLUMN_3 = "owner";
    public static final String COLUMN_4 = "created_at";

    //http://tips.androidhive.info/2013/10/android-insert-datetime-value-in-sqlite-database/2

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + SQLHelper.TABLE_NAME + " (" +
                    SQLHelper.COLUMN_3 + " TEXT NOT NULL," +
                    SQLHelper.COLUMN_4 + " TEXT NOT NULL," +
                    SQLHelper.COLUMN_1 + " TEXT NOT NULL," +
                    SQLHelper.COLUMN_2 + " TEXT PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE);";

    //PRIMARY KEY, UNIQUE (key) ON CONFLICT REPLACE

    private static final String SQL_CREATE_TEST =
            "CREATE TABLE " + SQLHelper.TEST_TABLE + " (" +
                    SQLHelper.COLUMN_1 + " TEXT PRIMARY KEY," +
                    SQLHelper.COLUMN_2 + " TEXT NOT NULL);";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + SQLHelper.TABLE_NAME;

    public SQLHelper(Context context){
        super(context,DATABASE_NAME,null,1);
    }
    @Override
    public void onCreate(SQLiteDatabase Db){
        Db.execSQL(SQL_CREATE_TEST);
        Db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase Db,int oldV, int newV){

    }

    public void onDelete(SQLiteDatabase Db) {
        Db.execSQL(SQL_DELETE_ENTRIES);

    }

    public ContentValues updateContentValue(ContentValues cValues, String key, String value){
        cValues.put("key", key);
        cValues.put("value", value);
        return cValues;
    }

}
