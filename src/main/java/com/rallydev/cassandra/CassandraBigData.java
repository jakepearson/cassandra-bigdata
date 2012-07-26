package com.rallydev.cassandra;

import org.apache.commons.beanutils.MethodUtils;

public class CassandraBigData {

    public static void main(String [] args) throws Exception{
        if(args.length == 0) {
            printHelp();
        } else {
            execute(args[0]);
        }
    }

    private static void execute(String action) throws Exception {
        MethodUtils.invokeMethod(new ActionRunner(), Action.byCommand(action).getMethod(), null);
    }

    private static void printHelp() {
        System.out.println("Please provide one of the following arguments: ");
        for(Action action : Action.values()) {
            System.out.println("   " + action.getCommand() + " (" + action.getDescription() + ")");
        }
    }

    private enum Action {
        CREATE("-create", "create", "Create 10^7 rows"),
        DELETE("-delete", "delete", "Delete all rows"),
        READ("-read", "read", "Read all rows"),
        RESET("-reset", "reset", "Delete the keyspace");


        private String command;
        private String method;
        private final String description;

        private Action(String command, String method, String description) {
            this.command = command;
            this.method = method;
            this.description = description;
        }

        public String getCommand() {
            return command;
        }

        public String getMethod() {
            return method;
        }

        public String getDescription() {
            return description;
        }

        public static Action byCommand(String command) {
            for (Action action : Action.values()) {
                if (action.getCommand().equals(command)) {
                    return action;
                }
            }
            throw new RuntimeException("Invalid action");
        }
    }
}
