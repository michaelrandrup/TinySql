$(document).ready(function () {
    $(".listnavigation").on("click", function (event) {
        event.preventDefault();
        var tableName = $(this).data("table");
        var listType = $(this).data("listtype");

        LoadList(tableName,listType,"","#tinysqllist");

    });
});