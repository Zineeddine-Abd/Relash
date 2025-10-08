package Main;

import Disk_Manager.DiskManager;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        // Charger config
        DBConfig config = DBConfig.loadFromFile("config/dbconfig.json");

        // Initialiser le gestionnaire disque
        DiskManager diskManager = new DiskManager(config);
        diskManager.Init();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Relash is live. Tapez vos commandes (EXIT pour quitter)");

        while (true) {
            System.out.print("> ");
            String cmd = scanner.nextLine();

            Object parsed = Parser.parse(cmd, config);

            if (parsed == null) {
                System.out.println("Commande non reconnue !");
                continue;
            }

            if (parsed.equals("EXIT")) {
                System.out.println("Terminated...");
                break;
            }

            if (parsed instanceof commandes.CreateTable) {
                ((commandes.CreateTable) parsed).execute();
            }
        }

        // Finaliser le gestionnaire disque
        diskManager.Finish();
        scanner.close();
    }
}