#include "async.h"

void callback(void *param, void *tres, int code)
{
    CB_DATA *cbData = (CB_DATA *)param;
    cbData->callback(tres, code);
}

void *init()
{
    return &callback;
}