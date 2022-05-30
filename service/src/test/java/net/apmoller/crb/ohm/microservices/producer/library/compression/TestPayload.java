package net.apmoller.crb.ohm.microservices.producer.library.compression;

public class TestPayload {

    public static String jsonPayload() {
        return "{\n" + "\t\"db_response\": {\n" + "\t\t\"container\": {\n" + "\t\t\t\"archive\": {\n"
                + "\t\t\t\t\"doctype\": \"\",\n" + "\t\t\t\t\"expirydate\": \"2022-10-01\",\n"
                + "\t\t\t\t\"index_s\": \"43e4be58-9c54-493f-ad40-6847bad741a5\",\n"
                + "\t\t\t\t\"code\": \"0A732E776A9C61AEEF6A802CD9F7\",\n"
                + "\t\t\t\t\"index_m\": \"43e4be58-9c54-493f-ad40-6847bad741a5\",\n" + "\t\t\t\t\"docid\": 500005964,\n"
                + "\t\t\t\t\"domain\": \"DKGIS\",\n" + "\t\t\t\t\"save\": true\n" + "\t\t\t}\n" + "\t\t},\n"
                + "\t\t\"response\": {\n" + "\t\t\t\"returncode\": 0,\n"
                + "\t\t\t\"origin\": \"DMS:SCRBDBKDK007206\",\n" + "\t\t\t\"error\": 0,\n"
                + "\t\t\t\"returnstring\": [\n" + "\t\t\t\t{\n" + "\t\t\t\t\t\"source\": \"Docengine\",\n"
                + "\t\t\t\t\t\"content\": \"Added ply [1] by index\"\n" + "\t\t\t\t},\n" + "\t\t\t\t{\n"
                + "\t\t\t\t\t\"source\": \"Docengine\",\n"
                + "\t\t\t\t\t\"content\": \"Could not find ply [2] by index\"\n" + "\t\t\t\t},\n" + "\t\t\t\t{\n"
                + "\t\t\t\t\t\"source\": \"DBDatabase\",\n" + "\t\t\t\t\t\"content\": \"Key_Add\"\n" + "\t\t\t\t},\n"
                + "\t\t\t\t{\n" + "\t\t\t\t\t\"source\": \"DBDatabase\",\n" + "\t\t\t\t\t\"content\": \"Key_Add\"\n"
                + "\t\t\t\t},\n" + "\t\t\t\t{\n" + "\t\t\t\t\t\"source\": \"DBDatabase\",\n"
                + "\t\t\t\t\t\"content\": \"Key_Add\"\n" + "\t\t\t\t},\n" + "\t\t\t\t\t\"source\": \"DBDatabase\",\n"
                + "\t\t\t\t\t\"content\": \"Key_Add\"\n" + "\t\t\t\t},\n" + "\t\t\t\t{\n"
                + "\t\t\t\t\t\"source\": \"DBDatabase\",\n" + "\t\t\t\t\t\"content\": \"Key_Add\"\n" + "\t\t\t\t}\n"
                + "\t\t\t]\n" + "\t\t},\n" + "\t\t\"type\": \"db_extract_package\",\n" + "\t\t\"version\": 2,\n"
                + "\t\t\"revision\": 0\n" + "\t}\n" + "}";
    }
}
