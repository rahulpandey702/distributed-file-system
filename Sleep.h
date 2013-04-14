#include <sys/select.h>
void Sleep_ms(int msecs) {
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = msecs*1000;
  select(0, NULL, NULL, NULL, &tv);
  return;
}

void Sleep(int nsec) {
  struct timeval tv;
  tv.tv_sec = nsec;
  tv.tv_usec = 0;
  select(0, NULL, NULL, NULL, &tv);
  return;
}
