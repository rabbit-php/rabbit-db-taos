typedef void (*__async_cb_func_t)(void *tres, int code);

typedef struct
{
    __async_cb_func_t callback;
} CB_DATA;

void *init();