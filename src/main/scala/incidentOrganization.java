import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class incidentOrganization {

    public static void main(String args[]) {
        File file = new File("data/temp");

        Scanner scanner;
        try {
            scanner = new Scanner(file);


            ArrayList<String> types = new ArrayList<String>();
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();

                if(!types.contains(line)) {
                    types.add(line);
                }
            }

            for(int i = 0; i < types.size(); i++) {
                System.out.println(types.get(i));
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
