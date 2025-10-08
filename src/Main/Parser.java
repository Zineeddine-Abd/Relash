package Main;

public class Parser {

    public static Object parse(String cmd, DBConfig config) {

        cmd = cmd.trim();

        if (cmd.toUpperCase().startsWith("CREATE TABLE")) {
            String[] parts = cmd.split("\\(", 2);
            String tableName = parts[0].split(" ")[2];
            String colsPart = parts[1].replace(")", "").trim();
            String[] columns = colsPart.split(",");

            for (int i = 0; i < columns.length; i++) {
                columns[i] = columns[i].trim();
            }

            return new commandes.CreateTable(tableName, columns, config);

        } else {

            if (cmd.toUpperCase().startsWith("SELECT")) {
                return new commandes.Select(cmd, config);
            } else if (cmd.toUpperCase().startsWith("INSERT INTO")) {

            }

        }

        if (cmd.equalsIgnoreCase("EXIT")) {
            return "EXIT";
        }

        return null;
    }
}
