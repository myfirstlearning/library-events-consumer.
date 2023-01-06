package com.libraryeventsconsumer.repository;

import com.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {



}
