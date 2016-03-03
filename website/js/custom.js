jQuery(document).ready(function($) {

    /************** Scroll Navigation *********************/
    $('.navigation').singlePageNav({
        currentClass: 'active'
    });


    /************** FlexSlider *********************/
    $('.flexslider').flexslider({
        animation: "fade",
        directionNav: false
    });


    /************** Responsive Navigation *********************/

    $('.menu-toggle-btn').click(function() {
        $('.responsive-menu').stop(true, true).slideToggle();
    });


    var heights = $(".well2").map(function() {
            return $(this).height();
        }).get(),

        maxHeight = Math.max.apply(null, heights);

    $(".well2").height(maxHeight);


    /************** Menu Content Opening *********************/
    $(".main_menu a, .responsive_menu a").click(function() {
        var id = $(this).attr('class');
        id = id.split('-');
        $("#menu-container .content").hide();
        $("#menu-container #menu-" + id[1]).addClass("animated fadeInDown").show();
        $("#menu-container .homepage").hide();
        $(".support").hide();
        $(".testimonials").hide();
        return false;
    });

    $(window).load(function() {
        $("#menu-container .products").hide();
    });

    $(".main_menu a.home").addClass('active');

    $(".main_menu a.home, .responsive_menu a.home").click(function() {
        $("#menu-container .homepage").addClass("animated fadeInDown").show();
        $(this).addClass('active');
        $(".main_menu a.page2, .responsive_menu a.page2").removeClass('active');
        $(".main_menu a.page3, .responsive_menu a.page3").removeClass('active');
        $(".main_menu a.page5, .responsive_menu a.page5").removeClass('active');
        return false;
    });

    $(".main_menu a.page2, .responsive_menu a.page2").click(function() {
        $("#menu-container .service").addClass("animated fadeInDown").show();
        $(this).addClass('active');
        $(".main_menu a.home, .responsive_menu a.home").removeClass('active');
        $(".main_menu a.page3, .responsive_menu a.page3").removeClass('active');
        $(".main_menu a.page5, .responsive_menu a.page5").removeClass('active');
        return false;
    });

    $(".main_menu a.page3, .responsive_menu a.page3").click(function() {
        $("#menu-container .portfolio").addClass("animated fadeInDown").show();
        $(".our-services").show();
        $(this).addClass('active');
        $(".main_menu a.page2, .responsive_menu a.page2").removeClass('active');
        $(".main_menu a.home, .responsive_menu a.home").removeClass('active');
        $(".main_menu a.page5, .responsive_menu a.page5").removeClass('active');
        return false;
    });

    $(".main_menu a.page5, .responsive_menu a.page5").click(function() {
        $("#menu-container .contact").addClass("animated fadeInDown").show();
        $(this).addClass('active');
        $(".main_menu a.page2, .responsive_menu a.page2").removeClass('active');
        $(".main_menu a.page3, .responsive_menu a.page3").removeClass('active');
        $(".main_menu a.home, .responsive_menu a.home").removeClass('active');

        return false;
    });




    /************** LightBox *********************/


    $("a.menu-toggle-btn").click(function() {
        $(".responsive_menu").stop(true, true).slideToggle();
        return false;
    });

    $(".responsive_menu a").click(function() {
        $('.responsive_menu').hide();
    });


    var $timeline_block = $('.cd-timeline-block');

    //hide timeline blocks which are outside the viewport
    $timeline_block.each(function() {
        if ($(this).offset().top > $(window).scrollTop() + $(window).height() * 0.75) {
            $(this).find('.cd-timeline-img, .cd-timeline-content').addClass('is-hidden');
        }
    });

    //on scolling, show/animate timeline blocks when enter the viewport
    $(window).on('scroll', function() {
        animationPage();
        $timeline_block.each(function() {
            if ($(this).offset().top <= $(window).scrollTop() + $(window).height() * 0.75 && $(this).find('.cd-timeline-img').hasClass('is-hidden')) {
                $(this).find('.cd-timeline-img, .cd-timeline-content').removeClass('is-hidden').addClass('bounce-in');
            }
        });




    });



});

$(".navbar-collapse").css({
    maxHeight: $(window).height() - $(".navbar-header").height() + "px"
});


function animationPage() {

    var scrollT = $(window).scrollTop();
    var currentPosition = scrollT + 480;
    var imageIcon = $('.usecaseimage').offset().top;
    if (imageIcon < currentPosition) {
        $(".usecaseimage").addClass('animated bounceIn');
    } else {
        $(".usecaseimage").removeClass('animated bounceIn');
    }

    var modules = $(".well2").offset().top;
    if (modules < currentPosition) {
        $(".well2").addClass('animated bounce');
    } else {
        $(".well2").removeClass('animated bounce');
    }

};