package com.github.makewheels.cfdatabasebackup;

import com.mongodb.client.MongoClient;

import java.io.FileOutputStream;

public class BackupService {

    public void backup() {
        MongoClientURI uri = new MongoClientURI(MONGO_URI);
        MongoClient mongoClient = new MongoClient(uri);
        String backupFileName = databaseName + "_backup_" + System.currentTimeMillis() + ".bson";
        FileOutputStream outputStream = new FileOutputStream("<backup_file_path>/" + backupFileName);
        mongoClient.getDatabase(databaseName).createMongoDump(outputStream);
        outputStream.close();
        mongoClient.close();
    }
}
