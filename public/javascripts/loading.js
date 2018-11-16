$(function () {
    console.log("LOADING");
    setTimeout(
        function(){
            console.log(">>> Reload...");
            location.reload(true);
        }, 3000);

});