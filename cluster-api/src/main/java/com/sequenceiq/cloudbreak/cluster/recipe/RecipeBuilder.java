package com.sequenceiq.cloudbreak.cluster.recipe;

import java.util.List;

import com.sequenceiq.cloudbreak.cluster.RecipeScript;
import com.sequenceiq.cloudbreak.domain.Recipe;

public interface RecipeBuilder {

    List<Recipe> buildRecipes(String name, List<RecipeScript> recipeScripts);

}
