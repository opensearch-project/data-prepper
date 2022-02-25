/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @since 1.3
 * <p>
 * Exception thrown by {@link ParseTreeParser} if ANTLR parse emits error events.
 */
public class CompositeException extends RuntimeException {

        private static final long serialVersionUID = 3026362227162912146L;

        private final List<Throwable> exceptions;
        private final String message;
        private Throwable cause;

        /**
         * Constructs a CompositeException with the given array of Throwables as the
         * list of suppressed exceptions.
         * @param exceptions the Throwables to have as initially suppressed exceptions
         *
         * @throws IllegalArgumentException if <code>exceptions</code> is empty.
         */
        public CompositeException(final Throwable... exceptions) {
            this(exceptions == null ?
                    Collections.singletonList(new NullPointerException("exceptions was null")) : Arrays.asList(exceptions));
        }

        /**
         * Constructs a CompositeException with the given array of Throwables as the
         * list of suppressed exceptions.
         * @param errors the Throwables to have as initially suppressed exceptions
         *
         * @throws IllegalArgumentException if <code>errors</code> is empty.
         */
        public CompositeException(final Iterable<? extends Throwable> errors) {
            final Set<Throwable> deDupedExceptions = new LinkedHashSet<>();
            if (errors != null) {
                for (final Throwable ex : errors) {
                    if (ex instanceof CompositeException) {
                        deDupedExceptions.addAll(((CompositeException) ex).getExceptions());
                    } else
                    if (ex != null) {
                        deDupedExceptions.add(ex);
                    } else {
                        deDupedExceptions.add(new NullPointerException("Throwable was null!"));
                    }
                }
            } else {
                deDupedExceptions.add(new NullPointerException("errors was null"));
            }
            if (deDupedExceptions.isEmpty()) {
                throw new IllegalArgumentException("errors is empty");
            }
            final List<Throwable> localExceptions = new ArrayList<>(deDupedExceptions);
            this.exceptions = Collections.unmodifiableList(localExceptions);
            this.message = exceptions.size() + " exceptions occurred. ";
        }

        /**
         * Retrieves the list of exceptions that make up the {@code CompositeException}.
         *
         * @return the exceptions that make up the {@code CompositeException}, as a {@link List} of {@link Throwable}s
         */
        public List<Throwable> getExceptions() {
            return exceptions;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public synchronized Throwable getCause() { // NOPMD
            if (cause == null) {
                final String separator = System.getProperty("line.separator");
                if (exceptions.size() > 1) {
                    final Map<Throwable, Boolean> seenCauses = new IdentityHashMap<>();

                    final StringBuilder aggregateMessage = new StringBuilder();
                    aggregateMessage.append("Multiple exceptions (").append(exceptions.size()).append(")").append(separator);

                    for (Throwable inner : exceptions) {
                        int depth = 0;
                        while (inner != null) {
                            for (int i = 0; i < depth; i++) {
                                aggregateMessage.append("  ");
                            }
                            aggregateMessage.append("|-- ");
                            aggregateMessage.append(inner.getClass().getCanonicalName()).append(": ");
                            final String innerMessage = inner.getMessage();
                            if (innerMessage != null && innerMessage.contains(separator)) {
                                aggregateMessage.append(separator);
                                for (final String line : innerMessage.split(separator)) {
                                    for (int i = 0; i < depth + 2; i++) {
                                        aggregateMessage.append("  ");
                                    }
                                    aggregateMessage.append(line).append(separator);
                                }
                            } else {
                                aggregateMessage.append(innerMessage);
                                aggregateMessage.append(separator);
                            }

                            for (int i = 0; i < depth + 2; i++) {
                                aggregateMessage.append("  ");
                            }
                            final StackTraceElement[] st = inner.getStackTrace();
                            if (st.length > 0) {
                                aggregateMessage.append("at ").append(st[0]).append(separator);
                            }

                            if (!seenCauses.containsKey(inner)) {
                                seenCauses.put(inner, true);

                                inner = inner.getCause();
                                depth++;
                            } else {
                                inner = inner.getCause();
                                if (inner != null) {
                                    for (int i = 0; i < depth + 2; i++) {
                                        aggregateMessage.append("  ");
                                    }
                                    aggregateMessage.append("|-- ");
                                    aggregateMessage.append("(cause not expanded again) ");
                                    aggregateMessage.append(inner.getClass().getCanonicalName()).append(": ");
                                    aggregateMessage.append(inner.getMessage());
                                    aggregateMessage.append(separator);
                                }
                                break;
                            }
                        }
                    }

                    cause = new ExceptionOverview(aggregateMessage.toString().trim());
                } else {
                    cause = exceptions.get(0);
                }
            }
            return cause;
        }

        /**
         * All of the following {@code printStackTrace} functionality is derived from JDK {@link Throwable}
         * {@code printStackTrace}. In particular, the {@code PrintStreamOrWriter} abstraction is copied wholesale.
         *
         * Changes from the official JDK implementation:<ul>
         * <li>no infinite loop detection</li>
         * <li>smaller critical section holding {@link PrintStream} lock</li>
         * <li>explicit knowledge about the exceptions {@link List} that this loops through</li>
         * </ul>
         */
        @Override
        public void printStackTrace() {
            printStackTrace(System.err);
        }

        @Override
        public void printStackTrace(final PrintStream s) {
            printStackTrace(new WrappedPrintStream(s));
        }

        @Override
        public void printStackTrace(final PrintWriter s) {
            printStackTrace(new WrappedPrintWriter(s));
        }

        /**
         * Special handling for printing out a {@code CompositeException}.
         * Loops through all inner exceptions and prints them out.
         *
         * @param output
         *            stream to print to
         */
        private void printStackTrace(final PrintStreamOrWriter output) {
            output.append(this).append("\n");
            for (final StackTraceElement myStackElement : getStackTrace()) {
                output.append("\tat ").append(myStackElement).append("\n");
            }
            int i = 1;
            for (final Throwable ex : exceptions) {
                output.append("  ComposedException ").append(i).append(" :\n");
                appendStackTrace(output, ex, "\t");
                i++;
            }
            output.append("\n");
        }

        private void appendStackTrace(final PrintStreamOrWriter output, final Throwable ex, final String prefix) {
            output.append(prefix).append(ex).append('\n');
            for (final StackTraceElement stackElement : ex.getStackTrace()) {
                output.append("\t\tat ").append(stackElement).append('\n');
            }
            if (ex.getCause() != null) {
                output.append("\tCaused by: ");
                appendStackTrace(output, ex.getCause(), "");
            }
        }

        abstract static class PrintStreamOrWriter {
            /**
             * Prints the object's string representation via the underlying PrintStream or PrintWriter.
             * @param o the object to print
             * @return this
             */
            abstract PrintStreamOrWriter append(Object o);
        }

        /**
         * Same abstraction and implementation as in JDK to allow PrintStream and PrintWriter to share implementation.
         */
        static final class WrappedPrintStream extends PrintStreamOrWriter {
            private final PrintStream printStream;

            WrappedPrintStream(final PrintStream printStream) {
                this.printStream = printStream;
            }

            @Override
            WrappedPrintStream append(final Object o) {
                printStream.print(o);
                return this;
            }
        }

        /**
         * Same abstraction and implementation as in JDK to allow PrintStream and PrintWriter to share implementation.
         */
        static final class WrappedPrintWriter extends PrintStreamOrWriter {
            private final PrintWriter printWriter;

            WrappedPrintWriter(final PrintWriter printWriter) {
                this.printWriter = printWriter;
            }

            @Override
            WrappedPrintWriter append(final Object o) {
                printWriter.print(o);
                return this;
            }
        }

        /**
         * Contains a formatted message with a simplified representation of the exception graph
         * contained within the CompositeException.
         */
        static final class ExceptionOverview extends RuntimeException {

            private static final long serialVersionUID = 3875212506787802066L;

            ExceptionOverview(final String message) {
                super(message);
            }

            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }
        }

        /**
         * Returns the number of suppressed exceptions.
         * @return the number of suppressed exceptions
         */
        public int size() {
            return exceptions.size();
        }
    }