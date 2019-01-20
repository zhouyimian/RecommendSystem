package com.km.rest;

import com.km.model.User;
import com.km.request.LoginUserRequest;
import com.km.request.RegisterUserRequest;
import com.km.request.UpdateUserGenresRequest;
import com.km.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/rest/user")
public class UserRestApi {

    @Autowired
    private UserService userService;

    //用户注册功能
    @GetMapping(value = "/register",produces = "application/json")
    @ResponseBody
    public Model registerUser(@RequestParam("username") String username,@RequestParam("password") String password, Model model){
        model.addAttribute("success",userService.registerUser(new RegisterUserRequest(username,password)));
        return model;
    }

    //用户登录功能
    @GetMapping(value = "/login", produces = "application/json")
    @ResponseBody
    public Model loginUser(@RequestParam("username") String username, @RequestParam("password") String password, Model model){
        model.addAttribute("success",userService.loginUser(new LoginUserRequest(username,password)));
        return model;
    }

    //添加用户喜欢电影类别
    @PostMapping("/addGenres")
    @ResponseBody
    public void addGenres(@RequestParam("username") String username, @RequestParam("genres") String genres, Model model){
        List<String> genresList = new ArrayList<>();
        for(String gen:genres.split("\\|"))
            genresList.add(gen);
        userService.updateUserGenres(new UpdateUserGenresRequest(username,genresList));
    }


}