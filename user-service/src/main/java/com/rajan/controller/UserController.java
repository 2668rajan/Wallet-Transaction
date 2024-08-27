package com.rajan.controller;

import com.rajan.dto.UserDto;
import com.rajan.service.UserService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/user-service")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/user")
    public Long createUser(@RequestBody @Valid UserDto userDto) throws ExecutionException, InterruptedException {

        return userService.createUser(userDto);
    }
}
