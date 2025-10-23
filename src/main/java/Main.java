import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length != 2 || !args[0].equals("-E")) {
            System.out.println("Usage: ./your_program.sh -E <pattern>");
            System.exit(1);
        }

        List<String> test = new ArrayList<>();

        String pattern = args[1];
        Scanner scanner = new Scanner(System.in);
        String inputLine = scanner.nextLine();

        if (handlePattern(inputLine, pattern)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    public static boolean handlePattern(String inputLine, String pattern) {

        if (pattern.equals("\\d")) {
            return handleDigitalCharacterClass(inputLine);
        }
        if (pattern.length() == 1) {
            return handleSingleLetterPattern(inputLine, pattern);
        }

        throw new RuntimeException("Unhandled pattern: " + pattern);
    }

    public static boolean handleDigitalCharacterClass(String inputLine) {
        final List<String> digits = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        return digits.stream().anyMatch(inputLine::contains);
    }

    public static boolean handleSingleLetterPattern(String inputLine, String pattern) {
        return inputLine.contains(pattern);
    }
}
