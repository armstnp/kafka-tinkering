package com.pariveda.kafka.helpers;

import avro.models.DataEvent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DataEventHelper {
    private static final String[] databases = {"xyz", "abc", "foo", "bar", "db1", "db2"};
    private static final String[] tables = {"customer", "user", "order", "status", "location"};

    private static final Random random = new Random();
    private static int eventId = 0;

    public static DataEvent generateDataEvent() {
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());

        DataEvent dataEvent = DataEvent.newBuilder()
                .setEventId(eventId++)
                .setTimestamp(timeStamp)
                .setDatabase(databases[random.nextInt(databases.length)])
                .setTable(tables[random.nextInt(tables.length)])
                .setColumn(generateRandomWord())
                .setOldValue(generateRandomWord())
                .setNewValue(generateRandomWord())
                .build();

        return dataEvent;
    }

    private static String generateRandomWord() {
        char[] word = new char[random.nextInt(8)+3];

        for(int i = 0; i < word.length; i++)
        {
            word[i] = (char)('a' + random.nextInt(26));
        }

        return new String(word);
    }
}
