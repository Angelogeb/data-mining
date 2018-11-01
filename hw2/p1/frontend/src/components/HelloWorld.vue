<template>
  <div class="search">
    <h1 class="pagetitle">{{ msg }}</h1>
    <form v-on:submit.prevent="performQuery" >
      <input v-model="query" placeholder="Search kijiji.it announcements">
      <p>Results retrieved in {{ retrieval_time }} s </p>
    </form>
    <ul>
      <li v-for="result in results" :key="result.id">
          <h3> <a :href=result.href> {{ result.title }} </a> | {{ result.city }}</h3>
          <small>score: {{ result.score }}</small>
          <p> {{ result.description }}</p>
      </li>
    </ul>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'HelloWorld',
  props: {
    msg: String
  },
  data() {
    return {
      query: '',
      retrieval_time: 0,
      results: []
    }
  },
  methods: {
    performQuery: function() {
      axios.post('/search', { query: this.query })
      .then(response => {
        this.results = response.data.documents
        this.retrieval_time = response.data.time
      })
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  margin: 0 10px;
}
a {
  background: #adffcf;
  color: black;
}

a:visited {
  color: #919192;
}

input {
  width: 100%;
  border-top: none;
  border-left: none;
  border-right: none;
  border-bottom-color: black;
  font-size: 1.5em;
  height: 1.5em;
}

form, h1 {
  padding: 0 10px;
}

.pagetitle {
  background: #adffcf;
  color: black;
}

.search {
  width: 60%;
  margin: auto;
}
</style>
