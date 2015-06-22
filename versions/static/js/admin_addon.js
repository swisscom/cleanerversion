function update_document() {
    // for the Source template:
    (function ($) {
        if ($('ul').hasClass('object-tools')) {
            $('input[name="_addanother"]').before('<input formaction="will_not_clone/" type="submit" formmethod="POST" value="Save without cloning">');
        }
    })(django.jQuery)


}


function make_restore_button(){
    (function($){
        if($('ul').hasClass('object-tools')){
            if($('div.field-is_current img').attr('alt').valueOf()=="False"){
                $('input[name="_addanother"]').before('<input formaction="restore/" type="submit" formmethod="POST" value="Restore">');
            }
        }
    })(django.jQuery)

}

// give time to jquery to load..
setTimeout("update_document();make_restore_button();", 1000);
