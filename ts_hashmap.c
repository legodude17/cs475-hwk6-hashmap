#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap.
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity)
{
  ts_hashmap_t *map = (ts_hashmap_t *)malloc(sizeof(ts_hashmap_t));
  map->capacity = capacity;
  map->numOps = 0;
  map->size = 0;
  // Use one lock per linked list, plus one for the numOps and size
  map->table = (ts_entry_t **)malloc(capacity * sizeof(ts_entry_t *));
  map->entryLocks = (pthread_mutex_t **)malloc(capacity * sizeof(pthread_mutex_t *));
  for (int i = 0; i < capacity; i++)
  {
    map->table[i] = NULL;
    map->entryLocks[i] = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(map->entryLocks[i], NULL);
  }
  map->propsLock = (pthread_spinlock_t *)malloc(sizeof(pthread_spinlock_t));
  pthread_spin_init(map->propsLock, PTHREAD_PROCESS_PRIVATE);
  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key)
{
  // Need to lock when we increment numOps
  pthread_spin_lock(map->propsLock);
  map->numOps++;
  pthread_spin_unlock(map->propsLock);
  unsigned int index = ((unsigned int)key) % map->capacity;
  pthread_mutex_lock(map->entryLocks[index]);
  ts_entry_t *entry = map->table[index];
  // Traverse the linked list, looking for the key
  while (entry != NULL)
  {
    if (entry->key == key)
    {
      int value = entry->value;
      pthread_mutex_unlock(map->entryLocks[index]);
      return value;
    }
    entry = entry->next;
  }
  pthread_mutex_unlock(map->entryLocks[index]);
  return INT_MAX;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value)
{
  unsigned int index = ((unsigned int)key) % map->capacity;
  pthread_mutex_lock(map->entryLocks[index]);
  ts_entry_t *entry = map->table[index];
  while (entry != NULL)
  {
    if (entry->key == key)
    {
      int oldValue = entry->value;
      entry->value = value;
      pthread_mutex_unlock(map->entryLocks[index]);
      pthread_spin_lock(map->propsLock);
      map->numOps++;
      pthread_spin_unlock(map->propsLock);
      return oldValue;
    }
    entry = entry->next;
  }
  entry = (ts_entry_t *)malloc(sizeof(ts_entry_t));
  entry->key = key;
  entry->value = value;
  entry->next = map->table[index];
  map->table[index] = entry;
  pthread_mutex_unlock(map->entryLocks[index]);
  pthread_spin_lock(map->propsLock);
  map->numOps++;
  map->size++;
  pthread_spin_unlock(map->propsLock);
  return INT_MAX;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key)
{
  pthread_spin_lock(map->propsLock);
  map->numOps++;
  map->size--;
  pthread_spin_unlock(map->propsLock);
  unsigned int index = ((unsigned int)key) % map->capacity;
  pthread_mutex_lock(map->entryLocks[index]);
  ts_entry_t *entry = map->table[index];
  ts_entry_t *prevEntry = NULL;
  while (entry != NULL)
  {
    if (entry->key == key)
    {
      if (prevEntry == NULL)
      {
        map->table[index] = entry->next;
      }
      else
      {
        prevEntry->next = entry->next;
      }
      int value = entry->value;
      free(entry);
      pthread_mutex_unlock(map->entryLocks[index]);
      return value;
    }
    prevEntry = entry;
    entry = entry->next;
  }
  pthread_mutex_unlock(map->entryLocks[index]);
  return INT_MAX;
}

/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map)
{
  for (int i = 0; i < map->capacity; i++)
  {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map)
{
  for (int i = 0; i < map->capacity; i++)
  {
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      ts_entry_t *next = entry->next;
      free(entry);
      entry = next;
    }
    pthread_mutex_destroy(map->entryLocks[i]);
    free(map->entryLocks[i]);
  }
  free(map->table);
  free(map->entryLocks);
  pthread_spin_destroy(map->propsLock);
  free(map->propsLock);
  free(map);
}
