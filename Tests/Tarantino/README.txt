/**
 *
 * USAGE: Inside 'WordFrequenciesClient.hpp', modify the SCRIPTS_DIR const to match your
 * directories structure (just run this until no 'IO Error' message appears).
 * If this runs slow on your computer, you can control the speed by changing the SLEEP_US
 * parameter in 'WordFrequenciesClient.hpp'.
 *
 * The test runs the word count job on 3 tarantino script files. The job gets 1 thread,
 * the second 2 threads, and the third 4 threads. Even though each one runs on a different file,
 * You should be able to see this clearly with the status updates.
 *
 * Verify the results with diff.
 *
 * This test is memory-leaks-free, so you can run it with valgrind and get reliable results.
 *
 * Written by: ???
 * Edited by: Kali, 05/05/2019
 *
 */