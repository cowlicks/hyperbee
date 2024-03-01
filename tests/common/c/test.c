/*
 * This file is used to test the rust ffi bindings.
 *
 * It requires a detector be running at ws://localhost:8081
 * Usually we test with the simulated standalone detector.
 */

#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>

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

char* print_arr(char* out, char* arr_buff, size_t len) {

    for (int i = 0; i < len; i++) {
        char next[sizeof(char)];
        sprintf(next, "%c", arr_buff[i]);
        strcat(out, next);
    }
    return out;
}

void callback(
        int64_t error_code,
        char* error_message,
        unsigned long long seq,
        char* key_buff,
        size_t key_length
    ) {
    printf("Hyperbee.get result:\n\tseq %d\n\tand array: ");
    char arr_str[255];
    print_arr(arr_str, key_buff, key_length);
    printf("%s\n", arr_str);
    deallocate_rust_string(error_message);
    deallocate_rust_buffer(key_buff, key_length);
    callback_not_called = 0;
}


int msleep(unsigned int tms) {
  return usleep(tms * 1000);
}

int test() {
    printf("Running C main\n");
    void* runtime = init_runtime();
    if (runtime == NULL) {
        printf("Failed to initialize runtime\n");
        return 1;
    }
    printf("Runtime ready in C\n");
    void* hyperbee = hyperbee_from_storage_directory(runtime, "/home/blake/git/hyperbee/test_data/basic/");
    if (hyperbee == NULL) {
        printf("Failed to initialize hyperbee\n");
        return 1;
    }

    sleep(1);
    char key_buf[1] = { '0' };
    size_t key_length = sizeof( key_buf );
    hb_get(runtime, hyperbee, key_buf, key_length, callback);
    msleep(1000);
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

    printf("Closing hyperbee from C\n");
    close_hyperbee(hyperbee);
    printf("Closing runtime from C\n");
    close_runtime(runtime);
    printf("Exiting C\n");
    return 0;
}

int main() {
    return test();
}
