package com.km.rest;

import com.km.java.model.Constant;
import com.km.model.*;
import com.km.request.*;
import com.km.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Controller
@RequestMapping("/rest/movies")
public class MovieRestApi {

    @Autowired
    private RecommenderService recommenderService;
    @Autowired
    private UserService userService;
    @Autowired
    private MovieService movieService;
    @Autowired
    private TagService tagService;
    @Autowired
    private RatingService ratingService;

    private Logger logger = LoggerFactory.getLogger(MovieRestApi.class);

    //====================================主页========================================
    //提供获取实时推荐信息接口
    @GetMapping("/stream")
    @ResponseBody
    public Model getRealtimeRecommendation(@RequestParam("username") String username, @RequestParam("num") int num, Model model) {
        User user = userService.findUserByUsername(username);
        List<Recommendation> recommendations = recommenderService.getStreamRecsMovies(new GetStreamRecsRequest(user.getUid(), num));
        //冷启动
        if (recommendations.size() == 0) {
            Random random = new Random();
            recommendations = recommenderService.getGenresTopMovies(new GetGenresTopMovieRequest(user.getGenres().get(random.nextInt(user.getGenres().size())), num));
        }

        List<Integer> ids = new ArrayList<>();
        for (Recommendation recommendation : recommendations) {
            ids.add(recommendation.getMid());
        }
        List<Movie> result = movieService.getMoviesByMids(ids);
        model.addAttribute("success", true);
        model.addAttribute("movies", result);
        return model;
    }

    //提供获取离线推荐信息接口
    @GetMapping("/offline")
    @ResponseBody
    public Model getOfflineRecommendation(@RequestParam("username") String username, @RequestParam("num") int num, Model model) {
        User user = userService.findUserByUsername(username);
        List<Recommendation> recommendations = recommenderService.getUserCFMovies(new GetUserCFRequest(user.getUid(), num));
        //冷启动
        if (recommendations.size() == 0) {
            Random random = new Random();
            recommendations = recommenderService.getGenresTopMovies(new GetGenresTopMovieRequest(user.getGenres().get(random.nextInt(user.getGenres().size())), num));
        }

        List<Integer> ids = new ArrayList<>();
        for (Recommendation recommendation : recommendations) {
            ids.add(recommendation.getMid());
        }
        List<Movie> result = movieService.getMoviesByMids(ids);
        model.addAttribute("success", true);
        model.addAttribute("movies", result);
        return model;
    }


    //提供获取热门推荐信息接口
    @GetMapping("/hot")
    @ResponseBody
    public Model getHotRecommendation(@RequestParam("num") int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService.getHotRecommendations(new GetHotRecommendationRequest(num)));
        return model;
    }

    //提供获取优质电影信息接口
    @GetMapping("/rate")
    @ResponseBody
    public Model getRateMoreRecommendation(@RequestParam("num") int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService.getRateModeMovies(new GetRateMoreMoviesRequest(num)));
        return model;
    }

    //提供最新电影信息接口
    @GetMapping("/new")
    @ResponseBody
    public Model getNewRecommendation(@RequestParam("num") int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService.getNewMovies(new GetNewMoviesRequest(num)));
        return model;
    }

    //=======================模糊检索=============================
    @GetMapping("/getFuzzySearchMovies")
    @ResponseBody
    public Model getFuzzySearchMovies(@RequestParam("query") String query,@RequestParam("num") int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService.getFuzzMovies(new GetFuzzySearchMoviesRequest(query,num)));
        return model;
    }

    //=====================单个电影详细页面=========================
    @GetMapping("/info/{mid}")
    @ResponseBody
    public Model getMovieInfo(@PathVariable("mid") int mid, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",movieService.findMovieInfo(mid));
        return model;
    }

    //给电影打标签
    @GetMapping("/addtag/{mid}")
    @ResponseBody
    public Model addTagToMovie(@PathVariable("mid") int mid, @RequestParam("username") String username ,@RequestParam("tagname") String tagname, Model model) {
        User user = userService.findUserByUsername(username);
        Tag tag =new Tag(user.getUid(),mid,tagname,System.currentTimeMillis()/1000);
        tagService.addTagToMovie(tag);
        return null;
    }

    //获取电影的所有标签信息
    @GetMapping("/tags/{mid}")
    @ResponseBody
    public Model getMovieTags(@PathVariable("mid") int mid, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",tagService.getMovieTags(mid));
        return model;
    }

    //获取电影的相似推荐电影推荐
    @GetMapping("/same/{mid}")
    @ResponseBody
    public Model getSimMoviesRecommendation(@PathVariable("mid") int mid,@RequestParam("num") int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService
                .getHybridRecommendations(new GetHybridRecommendationRequest(0.5,mid,num)));
        return model;
    }

    //给电影打分
    @PostMapping("/rate/{mid}")
    @ResponseBody
    public void rateMovie(@RequestParam("username") String username,@PathVariable("mid") int mid,
                           @RequestParam("score") Double score, Model model) {
        User user = userService.findUserByUsername(username);
        Rating rating = new Rating(user.getUid(),mid,score,System.currentTimeMillis()/1000);
        ratingService.rateToMovie(rating);
        //输出埋点日志
        logger.info(Constant.USER_RATING_LOG_PREFIX+rating.getUid()+"|"+rating.getMid()+"|"+rating.getScore()+"|"+rating.getTimestamp());
    }

    //提供影片类别查找
    @GetMapping("/genres")
    @ResponseBody
    public Model getGenresMovies(@RequestParam("genres") String genres, @RequestParam("num")int num, Model model) {
        model.addAttribute("success",true);
        model.addAttribute("movies",recommenderService
                .getGenresMovies(new GetGenresMovieRequest(genres,num)));
        return model;
    }

    //提供用户所有的电影评分记录
    @GetMapping("/getUserRatings")
    @ResponseBody
    public Model getUserRatings(@RequestParam("username") String uername, Model model) {
        return null;
    }

    //获取图表数据
    @GetMapping("/getUserChart")
    @ResponseBody
    public Model getUserChart(@RequestParam("username") String uername, Model model) {
        return null;
    }
}
