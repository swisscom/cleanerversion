function update_document() {
	// for the Source template:
    (function($) {
        if($('ul').hasClass('object-tools')) {
            $('input[name="_addanother"]').before('<input formaction="will_not_clone/" type="submit" formmethod="POST" value="Save without cloning">');
        }})(django.jQuery)


}

// give time to jquery to load..
setTimeout("update_document();", 1000);