<html>
   <body>


      <form action="/ajuploadfile" method="post" enctype="multipart/form-data">
          {{ csrf_field() }}

          <br />
          Please Select file to import data
          <br />
          <input type="file" name="ajfile" />
          <br /><br />
          <input type="submit" value="Upload" />
      </form>

   </body>
</html
