/*
 * This file is used to test the rust ffi bindings.
 *
 * It requires a detector be running at ws://localhost:8081
 * Usually we test with the simulated standalone detector.
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>

const char * REL_PATH_TO_BASIC_DATA = "tests/common/js/data/basic";

/* utils */
int msleep(unsigned int tms) {
  return usleep(tms * 1000);
}

char* print_arr(char* out, char* arr_buff, size_t len) {
    for (int i = 0; i < len; i++) {
        char next[sizeof(char)];
        sprintf(next, "%c", arr_buff[i]);
        strcat(out, next);
    }
    return out;
}

// run command and write output to `output`
int run_command(char* command, char* output, size_t max_size) {
    FILE *pf;

    // Setup our pipe for reading and execute our command.
    pf = popen(command,"r");

    if(!pf){
      fprintf(stderr, "Could not open pipe for output.\n");
      return 1;
    }

    // Grab data from process execution
    fgets(output, max_size , pf);

    if (pclose(pf) != 0) {
        fprintf(stderr," Error: Failed to close command stream \n");
        return 1;
    }

    return 0;
}
/* end utils */

/* ffi declerations */
typedef void (*HbGetResultCallback)(int64_t error_code, char* error_message, unsigned long long seq, char* buff, size_t length);

extern void* init_runtime();
extern void* close_runtime(void* runtime);
extern void* hyperbee_from_storage_directory(void* runtime, char* storage_directory);
extern void* close_hyperbee(void* hyperbee);
extern int32_t hb_get(
        void* runtime,
        void* hyperbee,
        char* key_buff,
        size_t key_length,
        HbGetResultCallback cb);

extern void* deallocate_rust_string(
        char* ptr);
extern void* deallocate_rust_buffer(
        char* ptr,
        size_t len);

int callback_not_called = 1;

void callback(
        int64_t error_code,
        char* error_message,
        unsigned long long seq,
        char* key_buff,
        size_t key_length
    ) {
    char arr_str[255];
    print_arr(arr_str, key_buff, key_length);
    printf("%s\n", arr_str);
    deallocate_rust_string(error_message);
    deallocate_rust_buffer(key_buff, key_length);
    callback_not_called = 0;
}
/* end ffi declerations */


int test() {
    char git_root[512];
    char storage_dir[512];

    void* runtime = init_runtime();
    if (runtime == NULL) {
        printf("Failed to initialize runtime\n");
        return 1;
    }

    run_command("git rev-parse --show-toplevel", git_root, sizeof(git_root));
    // trim trailing whitespace https://stackoverflow.com/a/71442959/1609380
    strtok(git_root, "\r\t\n ");
    sprintf(storage_dir, "%s/%s", git_root, REL_PATH_TO_BASIC_DATA);

    void* hyperbee = hyperbee_from_storage_directory(runtime, storage_dir);
    if (hyperbee == NULL) {
        printf("Failed to initialize hyperbee\n");
        return 1;
    }

    char key_buf[1] = { '0' };
    size_t key_length = sizeof( key_buf );
    hb_get(runtime, hyperbee, key_buf, key_length, callback);
    int count = 0;
    while (callback_not_called){
        count += 1;
        msleep(50);
        if (count > 100) {
            // timeout ~ 5 seconds
            printf("Five second timeout reached!");
            return 1;
        }
    }

    close_hyperbee(hyperbee);
    close_runtime(runtime);
    return 0;
}

int main() {
   return test();
}
