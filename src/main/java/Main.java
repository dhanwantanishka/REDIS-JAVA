import java.util.Scanner;

public class Main {

    private static char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    public static void main(String[] args) {
        if (args.length != 2 || !args[0].equals("-E")) {
            System.out.println("Usage: ./your_program.sh -E <pattern>");
            System.exit(1);
        }

        String pattern = args[1];
        Scanner scanner = new Scanner(System.in);
        String inputLine = scanner.nextLine();

        // You can use print statements as follows for debugging, they'll be visible when running
        // tests.
        System.err.println("Logs from your program will appear here!");

        String test = "apple123";

        if (matchPattern(inputLine, pattern)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    public static boolean matchPattern(String inputLine, String pattern) {
        if (pattern.length() != 1) {
            switch (pattern) {
                case "\\d":
                    for (int i = 0; i < inputLine.length(); i++) {
                        for (int j = 0; j < digits.length; j++) {
                            if (inputLine.charAt(i) == digits[j]) {
                                return true;
                            }
                        }
                    }
                    return false;
            }
            throw new RuntimeException("Unhandled pattern: " + pattern);
        }
        return inputLine.contains(pattern);
    }
}